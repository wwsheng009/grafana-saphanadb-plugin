import { quoteLiteral, unquoteIdentifier } from './sqlUtil';

export function buildTableQuery(dataset?: string, table?: string) {
  const database = dataset !== undefined ? quoteIdentAsLiteral(dataset) : 'database()';

  if (table) {
    table = table.replaceAll("*", "%")
    // if (!table.startsWith("\"")) {
    //   table = table.toUpperCase();
    // }
    return `SELECT table_name FROM tables WHERE schema_name = ${database} and table_name like '${table}' ORDER BY table_name limit 1000`;
  }
  return `SELECT table_name FROM tables WHERE schema_name = ${database} ORDER BY table_name limit 1000`;
}

export function showDatabases() {
  return `SELECT schema_name FROM SCHEMAS where schema_name not like '_SYS%' and schema_name <> 'SYS' ORDER BY schema_name`;
}

export function buildColumnQuery(table: string, dbName?: string) {
  let query = 'SELECT COLUMN_NAME, DATA_TYPE_NAME FROM TABLE_COLUMNS WHERE ';
  query += buildTableConstraint(table, dbName);

  query += ' ORDER BY column_name';

  return query;
}

export function buildTableConstraint(table: string, dbName?: string) {
  let query = '';

  // check for schema qualified table
  if (table.includes('.')) {
    const parts = table.split('.');
    query = 'schema_name = ' + quoteIdentAsLiteral(parts[0]);
    query += ' AND table_name = ' + quoteIdentAsLiteral(parts[1]);
    return query;
  } else {
    const database = dbName !== undefined ? quoteIdentAsLiteral(dbName) : 'database()';
    query = `schema_name = ${database} AND table_name = ` + quoteIdentAsLiteral(table);

    return query;
  }
}

export function quoteIdentAsLiteral(value: string) {
  return quoteLiteral(unquoteIdentifier(value));
}
