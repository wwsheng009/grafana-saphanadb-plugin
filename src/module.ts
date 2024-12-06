import { DataSourcePlugin } from '@grafana/data';
import { SapHanaDatasource } from './SapHanaDataSource';
import { ConfigurationEditor } from './configuration/ConfigurationEditor';
import { MySQLOptions } from './types';
import { SQLQuery, SqlQueryEditorLazy } from 'grafana-sql';

export const plugin = new DataSourcePlugin<SapHanaDatasource, SQLQuery, MySQLOptions>(SapHanaDatasource)
  .setConfigEditor(ConfigurationEditor)
  .setQueryEditor(SqlQueryEditorLazy);
