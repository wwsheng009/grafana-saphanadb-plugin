import { isEmpty } from 'lodash';

import { SQLQuery, createSelectClause, haveColumns } from 'grafana-sql';

export function toRawSql({ sql, dataset, table }: SQLQuery): string {
  let rawQuery = '';

  // Return early with empty string if there is no sql column
  if (!sql || !haveColumns(sql.columns)) {
    return rawQuery;
  }

  rawQuery += createSelectClause(sql.columns);

  if (dataset && table) {
    rawQuery += `FROM ${dataset}.${table} `;
  }

  if (sql.whereString) {
    rawQuery += `WHERE ${sql.whereString} `;
  }

  if (sql.groupBy?.[0]?.property.name) {
    const groupBy = sql.groupBy.map((g) => g.property.name).filter((g) => !isEmpty(g));
    rawQuery += `GROUP BY ${groupBy.join(', ')} `;
  }

  if (sql.orderBy?.property.name) {
    rawQuery += `ORDER BY ${sql.orderBy.property.name} `;
  }

  if (sql.orderBy?.property.name && sql.orderByDirection) {
    rawQuery += `${sql.orderByDirection} `;
  }

  // Altough LIMIT 0 doesn't make sense, it is still possible to have LIMIT 0
  if (sql.limit !== undefined && sql.limit >= 0) {
    rawQuery += `LIMIT ${sql.limit} `;
  }
  return rawQuery;
}

// Puts backticks (`) around the identifier if it is necessary.
export function quoteIdentifierIfNecessary(value: string) {
  return isValidIdentifier(value) ? value : `"${value}"`;
}

/**
 * Validates the identifier from HANA and returns true if it
 * doesn't need to be escaped.
 */
export function isValidIdentifier(identifier: string): boolean {
  // const isValidName = /^[a-zA-Z_][a-zA-Z0-9_$]*$/g.test(identifier);
  const isValidName = /^[A-Z_][A-Z0-9_$]*$/g.test(identifier);

  const isReservedWord = RESERVED_WORDS.includes(identifier.toUpperCase());
  return !isReservedWord && isValidName;
}

// remove identifier quoting from identifier to use in metadata queries
export function unquoteIdentifier(value: string) {
  if (value[0] === '"' && value[value.length - 1] === '"') {
    return value.substring(1, value.length - 1).replace(/""/g, '"');
  } else if (value[0] === '`' && value[value.length - 1] === '`') {
    return value.substring(1, value.length - 1);
  } else {
    return value;
  }
}

export function quoteLiteral(value: string) {
  return "'" + value.replace(/'/g, "''") + "'";
}

/**
 * SELECT * FROM RESERVED_KEYWORDS ORDER BY RESERVED_KEYWORD
 */
const RESERVED_WORDS = [
  'ALL',
  'ALTER',
  'AS',
  'BEFORE',
  'BEGIN',
  'BOTH',
  'CASE',
  'CHAR',
  'CONDITION',
  'CONNECT',
  'CROSS',
  'CUBE',
  'CURRENT_CONNECTION',
  'CURRENT_DATE',
  'CURRENT_SCHEMA',
  'CURRENT_TIME',
  'CURRENT_TIMESTAMP',
  'CURRENT_TRANSACTION_ISOLATION_LEVEL',
  'CURRENT_USER',
  'CURRENT_UTCDATE',
  'CURRENT_UTCTIME',
  'CURRENT_UTCTIMESTAMP',
  'CURRVAL',
  'CURSOR',
  'DECLARE',
  'DISTINCT',
  'ELSE',
  'ELSEIF',
  'END',
  'EXCEPT',
  'EXCEPTION',
  'EXEC',
  'FALSE',
  'FOR',
  'FROM',
  'FULL',
  'GROUP',
  'HAVING',
  'IF',
  'IN',
  'INNER',
  'INOUT',
  'INTERSECT',
  'INTO',
  'IS',
  'JOIN',
  'LATERAL',
  'LEADING',
  'LEFT',
  'LIMIT',
  'LOOP',
  'MINUS',
  'NATURAL',
  'NCHAR',
  'NEXTVAL',
  'NULL',
  'ON',
  'ORDER',
  'OUT',
  'PRIOR',
  'RETURN',
  'RETURNS',
  'REVERSE',
  'RIGHT',
  'ROLLUP',
  'ROWID',
  'SELECT',
  'SESSION_USER',
  'SET',
  'SQL',
  'START',
  'SYSUUID',
  'TABLESAMPLE',
  'TOP',
  'TRAILING',
  'TRUE',
  'UNION',
  'UNKNOWN',
  'USING',
  'UTCTIMESTAMP',
  'VALUES',
  'WHEN',
  'WHERE',
  'WHILE',
  'WITH'
];
