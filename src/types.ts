import { SQLOptions, SQLQuery } from 'grafana-sql';

export interface HANAOptions extends SQLOptions {
  allowCleartextPasswords?: boolean;
}

export interface HANAQuery extends SQLQuery { }
