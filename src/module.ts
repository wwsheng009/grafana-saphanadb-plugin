import { DataSourcePlugin } from '@grafana/data';
import { SapHanaDatasource } from './SapHanaDataSource';
import { ConfigurationEditor } from './configuration/ConfigurationEditor';
import { HANAOptions } from './types';
import { SQLQuery, SqlQueryEditorLazy } from 'grafana-sql';

export const plugin = new DataSourcePlugin<SapHanaDatasource, SQLQuery, HANAOptions>(SapHanaDatasource)
  .setConfigEditor(ConfigurationEditor)
  .setQueryEditor(SqlQueryEditorLazy);
