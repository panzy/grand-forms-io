const express = require('express');
const router = express.Router();

const logger = require('logger').createLogger();

const NodeJDBC = require('nodejdbc');

////////////////////////////////////////////////////////////////////////////////
// init
logger.setLevel(process.env.LOG_LEVEL || 'debug');

////////////////////////////////////////////////////////////////////////////////
// routes

/* GET users listing. */
router.get('/', function(req, res, next) {
  res.send('api index');
});

router.post('/submit', handleSubmitPost);

function handleSubmitPost(req, res) {
  // 用户提交了表单
  res.setHeader('content-type', 'text/plain');
  if (req.headers['content-type'] !== 'application/json') {
    res.status(415).send('invalid content-type, expect application/json, actual ' + req.headers['content-type'] + '.');
  } else if (!req.body) {
    res.status(400).send('invalid post body');
  } else {
    logger.debug('receive POST body', req.body);
    if (!req.body || !req.body.schema || !req.body.destination || !req.body.data) {
      res.status(400).send('require {schema, destination, data}');
      return;
    }
    writeDb(req, req.body).then(() => {
      res.status(204).end();
    }).catch(e => {
      logger.error(e);
      res.status(500).send(shortenErrorMessage(e.message));
    });
  }
}

/**
 * 执行 SQL 语句模板，支持在模板中嵌入变量。
 *
 * SQL 模板中的变量写法为：
 *
 *   {<name>[:<type]}
 *
 * 如
 *
 *   SELECT NAME FROM PERSONS WHERE ID={id:string}
 *
 * 支持的 types:
 *
 *  - string
 *  - number
 *  - int (is alias to number)
 *  - long
 *
 * @param {Connection} conn
 * @param {string} sql - SQL template
 * @param {object} params - parameter values, e.g. {id: "000000000000000"}
 * @param {function} cb - callback, accept a single paramter of type
 * ResultSet
 * @return {Promise} Resolved with {statement, query}, so you can call
 * statement.executeQuery(query).
 */
function prepareStatement(conn, sql, params, cb) {
  let query = sql.replace(/\{[a-z0-9_:]+\}/gi, '?');
  logger.debug('query to prepare statement with:', query);

  return conn.prepareStatement(query)
    .then(statement => {
      // Supplying Values for PreparedStatement Parameters
      let vars = sql.match(/\{[a-z0-9_]+(:[a-z]+)?\}/gi)
      if (vars) {
        // example: vars = [ '{Id}', '{age:int}' ]
        for (let i = 0; i < vars.length; ++i) {
          let [name, type] = vars[i].substr(1, vars[i].length - 2).split(':')
          if (!type)
            type = 'string'

          if (typeof params[name] === 'undefined') {
            throw `Parameter ${name} is not supplied. SQL template: ${sql}.`;
          }

          if (type === 'number' || type === 'int' || type === 'integer') {
            statement.setInt(i + 1, parseInt(params[name]));
          } else if (type === 'string') {
            statement.setString(i + 1, params[name].toString());
          } else if (type === 'long') {
            // parseInt 会把大数字解析成 double，所以这里不能调用
            // setLongSync，而要调用 setDoubleSync.
            statement.preparedStatement.setDoubleSync(i + 1, parseInt(params[name]));
          } else if (type === 'boolean') { // true/false => 1/0
            statement.setInt(i + 1, params[name] ? 1 : 0);
          } else {
            throw `Parameter ${name} is of unexpected type (${type}). SQL template: ${sql}.`;
          }
        }
      }

      return {statement, query};
    });
}

/**
 * 生成 INSERT SQL 模板，比如
 *
 * 'INSERT INTO todo (title,done) value ({title:string},{done:boolean})'
 *
 * @arg {object} schema form schema
 * @arg {object} destination form destination configuration
 * @arg {object} data form data
 * @return {string}
 */
function makeInsertSqlTemplate(schema, destination, data) {
  var table = destination.table;
  var fields = [], values = [];
  if (schema.type === 'object') {
    Object.entries(schema.properties).forEach(arr => {
      var name = arr[0]; // field name
      var desc = arr[1]; // field description
      if (typeof data[name] !== 'undefined') {
        fields.push(name);
        values.push('{' + name + ':' + desc.type + '}');
      }
    });
    var sql = `INSERT INTO ${table} (${fields.join(',')}) value (${values.join(',')})`;
    logger.debug(`insert template & params: ${sql},`, data);
    return Promise.resolve(sql);
  } else {
    return Promise.reject(new Error(`Form data with schema.type of "${schema.type}" is not allowed to write to database.`));
  }
}

function makeJdbcConfig(url) {
  if (url.startsWith('jdbc:mysql:')) {
    return {
      libs: [ (process.env.JAVA_SHARE || '/usr/share/java') + '/mysql-connector-java-5.1.38-bin.jar' ],
      className: className = 'com.mysql.jdbc.Driver',
      url,
    };
  }
  return null;
}

/**
 * 只保留错误消息的前两行，不足两行的消息不受影响。
 *
 * 这主要是为了精简包含了 stack trace 的错误消息。比如
 *
 * Error: Error running instance method
 * com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException: Table 'grandforms.todox' doesn't exist
 * 	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
 * 	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
 * 	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
 * 	at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
 * 	at com.mysql.jdbc.Util.handleNewInstance(Util.java:404)
 * 	...
 *
 * 变成了
 *
 * Error: Error running instance method
 * com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException: Table 'grandforms.todox' doesn't exist
 *
 * @arg {string} message
 * @return {string}
 */
function shortenErrorMessage(message) {
  return message.split('\n').slice(0, 2).join('\n');
}

function writeDb(req, arg) {
  var conf = makeJdbcConfig(arg.destination.url);
  if (!conf) {
    return Promise.reject(new Error('not supported JDBC URL'));
  }
  var nodejdbc = new NodeJDBC(conf);
  return makeInsertSqlTemplate(arg.schema, arg.destination, arg.data).then(sql =>
    nodejdbc.getConnection().then(conn =>
      prepareStatement(conn, sql, arg.data).then(r => r.statement.executeUpdate(r.query))
    )
  );
}

module.exports = router;

