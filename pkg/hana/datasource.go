package hana

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"

	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/SAP/go-hdb/driver"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
	"github.com/vincent/sap-hanadb/pkg/hana/sqleng"

	sdkhttpclient "github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
)

const (
	dateFormat      = "2006-01-02"
	dateTimeFormat1 = "2006-01-02 15:04:05"
	dateTimeFormat2 = "2006-01-02T15:04:05Z"
)

// Make sure Datasource implements required interfaces. This is important to do
// since otherwise we will only get a not implemented error response from plugin in
// runtime. In this example datasource instance implements backend.QueryDataHandler,
// backend.CheckHealthHandler interfaces. Plugin should not implement all these
// interfaces - only those which are required for a particular task.
// var (
//
//	_ backend.QueryDataHandler      = (*Datasource)(nil)
//	_ backend.CheckHealthHandler    = (*Datasource)(nil)
//	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
//
// )
func characterEscape(s string, escapeChar string) string {
	return strings.ReplaceAll(s, escapeChar, url.QueryEscape(escapeChar))
}

func NewInstanceSettings(logger log.Logger) datasource.InstanceFactoryFunc {
	return func(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
		cfg := backend.GrafanaConfigFromContext(ctx)
		sqlCfg, err := cfg.SQL()
		if err != nil {
			return nil, err
		}
		jsonData := sqleng.JsonData{
			MaxOpenConns:            sqlCfg.DefaultMaxOpenConns,
			MaxIdleConns:            sqlCfg.DefaultMaxIdleConns,
			ConnMaxLifetime:         sqlCfg.DefaultMaxConnLifetimeSeconds,
			SecureDSProxy:           false,
			AllowCleartextPasswords: false,
		}

		err = json.Unmarshal(settings.JSONData, &jsonData)
		if err != nil {
			return nil, fmt.Errorf("error reading settings: %w", err)
		}

		database := jsonData.Database
		if database == "" {
			database = settings.Database
		}

		dsInfo := sqleng.DataSourceInfo{
			JsonData:                jsonData,
			URL:                     settings.URL,
			User:                    settings.User,
			Database:                database,
			ID:                      settings.ID,
			Updated:                 settings.Updated,
			UID:                     settings.UID,
			DecryptedSecureJSONData: settings.DecryptedSecureJSONData,
		}

		// protocol := "tcp"
		// if strings.HasPrefix(dsInfo.URL, "/") {
		// 	protocol = "unix"
		// }

		// proxyClient, err := settings.ProxyClient(ctx)
		// if err != nil {
		// 	return nil, err
		// }

		// register the secure socks proxy dialer context, if enabled
		// if proxyClient.SecureSocksProxyEnabled() {
		// 	dialer, err := proxyClient.NewSecureSocksProxyContextDialer()
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	// UID is only unique per org, the only way to ensure uniqueness is to do it by connection information
		// 	uniqueIdentifier := dsInfo.User + dsInfo.DecryptedSecureJSONData["password"] + dsInfo.URL + dsInfo.Database
		// 	protocol, err = registerProxyDialerContext(protocol, uniqueIdentifier, dialer)
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// }

		// 支持的参数
		// databaseName
		// defaultSchema
		// timeout
		// pingInterval

		// TLSRootCAFile
		// TLSServerName
		// TLSInsecureSkipVerify

		// "hdb://<USER>:<PASSWORD>@something.hanacloud.ondemand.com:443?TLSServerName=something.hanacloud.ondemand.com"

		cnnstr := fmt.Sprintf("hdb://%s:%s@%s?databaseName=%s",
			characterEscape(dsInfo.User, ":"),
			dsInfo.DecryptedSecureJSONData["password"],
			characterEscape(dsInfo.URL, ")"),
			characterEscape(dsInfo.Database, "?"),
		)

		if dsInfo.JsonData.DefaultSchema != "" {
			cnnstr += fmt.Sprintf("&defaultSchema=%s", url.QueryEscape(dsInfo.JsonData.DefaultSchema))
		}

		// if dsInfo.JsonData.AllowCleartextPasswords {
		// 	cnnstr += "&allowCleartextPasswords=true"
		// }

		opts, err := settings.HTTPClientOptions(ctx)
		if err != nil {
			return nil, err
		}

		tlsConfig, err := sdkhttpclient.GetTLSConfig(opts)
		if err != nil {
			return nil, err
		}

		if tlsConfig.RootCAs != nil || len(tlsConfig.Certificates) > 0 {
			// tlsConfigString := fmt.Sprintf("ds%d", settings.ID)
			// if err := mysql.RegisterTLSConfig(tlsConfigString, tlsConfig); err != nil {
			// 	return nil, err
			// }
			// cnnstr += "&TLSInsecureSkipVerify=false&TLSRootCAFile=";
		} else if tlsConfig.InsecureSkipVerify {
			cnnstr += "&TLSInsecureSkipVerify=true"
		}

		// if dsInfo.JsonData.Timezone != "" {
		// 	cnnstr += fmt.Sprintf("&time_zone='%s'", url.QueryEscape(dsInfo.JsonData.Timezone))
		// }

		config := sqleng.DataPluginConfiguration{
			DSInfo:            dsInfo,
			TimeColumnNames:   []string{"time", "time_sec"},
			MetricColumnTypes: []string{"CHAR", "VARCHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"},
			RowLimit:          sqlCfg.RowLimit,
		}

		userFacingDefaultError, err := cfg.UserFacingDefaultError()
		if err != nil {
			return nil, err
		}

		rowTransformer := mysqlQueryResultTransformer{
			userError: userFacingDefaultError,
		}

		db, err := sql.Open("hdb", cnnstr)
		if err != nil {
			return nil, err
		}

		db.SetMaxOpenConns(config.DSInfo.JsonData.MaxOpenConns)
		db.SetMaxIdleConns(config.DSInfo.JsonData.MaxIdleConns)
		db.SetConnMaxLifetime(time.Duration(config.DSInfo.JsonData.ConnMaxLifetime) * time.Second)

		return sqleng.NewQueryDataHandler(userFacingDefaultError, db, config, &rowTransformer, newMysqlMacroEngine(logger, userFacingDefaultError), logger)
	}
}

// // NewDatasource creates a new datasource instance.
// func NewDatasource(_ context.Context, _ backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
// 	return &Datasource{}, nil
// }

// // Datasource is an example datasource which can respond to data queries, reports
// // its health and has streaming skills.
// type Datasource struct{}

// // Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// // created. As soon as datasource settings change detected by SDK old datasource instance will
// // be disposed and a new one will be created using NewSampleDatasource factory function.
// func (d *Datasource) Dispose() {
// 	// Clean up datasource instance resources.
// }

// // QueryData handles multiple queries and returns multiple responses.
// // req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// // The QueryDataResponse contains a map of RefID to the response for each query, and each response
// // contains Frames ([]*Frame).
// func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
// 	// create response struct
// 	response := backend.NewQueryDataResponse()

// 	// loop over queries and execute them individually.
// 	for _, q := range req.Queries {
// 		res := d.query(ctx, req.PluginContext, q)

// 		// save the response in a hashmap
// 		// based on with RefID as identifier
// 		response.Responses[q.RefID] = res
// 	}

// 	return response, nil
// }

// type queryModel struct{}

// func (d *Datasource) query(_ context.Context, pCtx backend.PluginContext, query backend.DataQuery) backend.DataResponse {
// 	var response backend.DataResponse

// 	// Unmarshal the JSON into our queryModel.
// 	var qm queryModel

// 	err := json.Unmarshal(query.JSON, &qm)
// 	if err != nil {
// 		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("json unmarshal: %v", err.Error()))
// 	}

// 	// create data frame response.
// 	// For an overview on data frames and how grafana handles them:
// 	// https://grafana.com/developers/plugin-tools/introduction/data-frames
// 	frame := data.NewFrame("response")

// 	// add fields.
// 	frame.Fields = append(frame.Fields,
// 		data.NewField("time", nil, []time.Time{query.TimeRange.From, query.TimeRange.To}),
// 		data.NewField("values", nil, []int64{10, 20}),
// 	)

// 	// add the frames to the response.
// 	response.Frames = append(response.Frames, frame)

// 	return response
// }

// // CheckHealth handles health checks sent from Grafana to the plugin.
// // The main use case for these health checks is the test button on the
// // datasource configuration page which allows users to verify that
// // a datasource is working as expected.
// func (d *Datasource) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
// 	res := &backend.CheckHealthResult{}
// 	config, err := models.LoadPluginSettings(*req.PluginContext.DataSourceInstanceSettings)

// 	if err != nil {
// 		res.Status = backend.HealthStatusError
// 		res.Message = "Unable to load settings"
// 		return res, nil
// 	}

// 	if config.Secrets.ApiKey == "" {
// 		res.Status = backend.HealthStatusError
// 		res.Message = "API key is missing"
// 		return res, nil
// 	}

// 	return &backend.CheckHealthResult{
// 		Status:  backend.HealthStatusOk,
// 		Message: "Data source is working",
// 	}, nil
// }

type mysqlQueryResultTransformer struct {
	userError string
}

func (t *mysqlQueryResultTransformer) TransformQueryError(logger log.Logger, err error) error {
	var driverErr driver.Error
	if errors.As(err, &driverErr) {
		// if driverErr != mysqlerr.ER_PARSE_ERROR && driverErr.Number != mysqlerr.ER_BAD_FIELD_ERROR &&
		// 	driverErr.Number != mysqlerr.ER_NO_SUCH_TABLE {
		logger.Error("Query error", "error", err)
		// 	return fmt.Errorf(("query failed - %s"), t.userError)
		// }
		return fmt.Errorf(("query failed - (%d):%s"), driverErr.Code(), driverErr.Text())
	}

	return err
}
func (t *mysqlQueryResultTransformer) GetConverterList2() []sqlutil.Converter {

	var fixedPattern = regexp.MustCompile(`FIXED\d{1,2}`)

	return []sqlutil.Converter{
		{
			Name: "handle DECIMAL",
			// InputScanKind:  reflect.Slice,
			// InputTypeName: "FIXED8",
			InputTypeRegex: fixedPattern,
			FrameConverter: sqlutil.FrameConverter{
				FieldType: data.FieldTypeFloat64,
				ConverterFunc: func(in interface{}) (interface{}, error) {

					rat, ok := in.(*driver.Decimal)
					if !ok {
						return nil, fmt.Errorf("value is not of type *driver.Decimal")
					}
					// Step 2: Convert *big.Rat to *string
					bigRat, _ := rat.Value()
					rat1, ok := bigRat.(*big.Rat)
					if !ok {
						return nil, fmt.Errorf("decimal: invalid data type %T", bigRat)
					}
					// Step 2: Convert *big.Rat to *string
					strValue, _ := rat1.Float64()
					// return 10.1,nil
					return strValue, nil
				},
			},
		},
	}
}
func (t *mysqlQueryResultTransformer) GetConverterList() []sqlutil.StringConverter {
	// For the MySQL driver , we have these possible data types:
	// https://www.w3schools.com/sql/sql_datatypes.asp#:~:text=In%20MySQL%20there%20are%20three,numeric%2C%20and%20date%20and%20time.
	// Since by default, we convert all into String, we need only to handle the Numeric data types
	return []sqlutil.StringConverter{
		{
			Name:           "handle DOUBLE",
			InputScanKind:  reflect.Struct,
			InputTypeName:  "DOUBLE",
			ConversionFunc: func(in *string) (*string, error) { return in, nil },
			Replacer: &sqlutil.StringFieldReplacer{
				OutputFieldType: data.FieldTypeNullableFloat64,
				ReplaceFunc: func(in *string) (any, error) {
					if in == nil {
						return nil, nil
					}
					v, err := strconv.ParseFloat(*in, 64)
					if err != nil {
						return nil, err
					}
					return &v, nil
				},
			},
		},
		{
			Name:           "handle BIGINT",
			InputScanKind:  reflect.Struct,
			InputTypeName:  "BIGINT",
			ConversionFunc: func(in *string) (*string, error) { return in, nil },
			Replacer: &sqlutil.StringFieldReplacer{
				OutputFieldType: data.FieldTypeNullableInt64,
				ReplaceFunc: func(in *string) (any, error) {
					if in == nil {
						return nil, nil
					}
					v, err := strconv.ParseInt(*in, 10, 64)
					if err != nil {
						return nil, err
					}
					return &v, nil
				},
			},
		},
		// {
		// 	Name:           "handle DECIMAL",
		// 	InputScanKind:  reflect.Slice,
		// 	InputTypeName:  "DECIMAL",
		// 	ConversionFunc: func(in *string) (*string, error) { return in, nil },
		// 	Replacer: &sqlutil.StringFieldReplacer{
		// 		OutputFieldType: data.FieldTypeNullableFloat64,
		// 		ReplaceFunc: func(in *string) (any, error) {
		// 			if in == nil {
		// 				return nil, nil
		// 			}
		// 			v, err := strconv.ParseFloat(*in, 64)
		// 			if err != nil {
		// 				return nil, err
		// 			}
		// 			return &v, nil
		// 		},
		// 	},
		// },
		{
			Name:           "handle DATETIME",
			InputScanKind:  reflect.Struct,
			InputTypeName:  "DATETIME",
			ConversionFunc: func(in *string) (*string, error) { return in, nil },
			Replacer: &sqlutil.StringFieldReplacer{
				OutputFieldType: data.FieldTypeNullableTime,
				ReplaceFunc: func(in *string) (any, error) {
					if in == nil {
						return nil, nil
					}
					v, err := time.Parse(dateTimeFormat1, *in)
					if err == nil {
						return &v, nil
					}
					v, err = time.Parse(dateTimeFormat2, *in)
					if err == nil {
						return &v, nil
					}

					return nil, err
				},
			},
		},
		{
			Name:           "handle DATE",
			InputScanKind:  reflect.Struct,
			InputTypeName:  "DATE",
			ConversionFunc: func(in *string) (*string, error) { return in, nil },
			Replacer: &sqlutil.StringFieldReplacer{
				OutputFieldType: data.FieldTypeNullableTime,
				ReplaceFunc: func(in *string) (any, error) {
					if in == nil {
						return nil, nil
					}
					v, err := time.Parse(dateFormat, *in)
					if err == nil {
						return &v, nil
					}
					v, err = time.Parse(dateTimeFormat1, *in)
					if err == nil {
						return &v, nil
					}
					v, err = time.Parse(dateTimeFormat2, *in)
					if err == nil {
						return &v, nil
					}
					return nil, err
				},
			},
		},
		{
			Name:           "handle TIMESTAMP",
			InputScanKind:  reflect.Struct,
			InputTypeName:  "TIMESTAMP",
			ConversionFunc: func(in *string) (*string, error) { return in, nil },
			Replacer: &sqlutil.StringFieldReplacer{
				OutputFieldType: data.FieldTypeNullableTime,
				ReplaceFunc: func(in *string) (any, error) {
					if in == nil {
						return nil, nil
					}
					v, err := time.Parse(dateTimeFormat1, *in)
					if err == nil {
						return &v, nil
					}
					v, err = time.Parse(dateTimeFormat2, *in)
					if err == nil {
						return &v, nil
					}
					return nil, err
				},
			},
		},
		{
			Name:           "handle YEAR",
			InputScanKind:  reflect.Struct,
			InputTypeName:  "YEAR",
			ConversionFunc: func(in *string) (*string, error) { return in, nil },
			Replacer: &sqlutil.StringFieldReplacer{
				OutputFieldType: data.FieldTypeNullableInt64,
				ReplaceFunc: func(in *string) (any, error) {
					if in == nil {
						return nil, nil
					}
					v, err := strconv.ParseInt(*in, 10, 64)
					if err != nil {
						return nil, err
					}
					return &v, nil
				},
			},
		},
		{
			Name:           "handle TINYINT",
			InputScanKind:  reflect.Struct,
			InputTypeName:  "TINYINT",
			ConversionFunc: func(in *string) (*string, error) { return in, nil },
			Replacer: &sqlutil.StringFieldReplacer{
				OutputFieldType: data.FieldTypeNullableInt64,
				ReplaceFunc: func(in *string) (any, error) {
					if in == nil {
						return nil, nil
					}
					v, err := strconv.ParseInt(*in, 10, 64)
					if err != nil {
						return nil, err
					}
					return &v, nil
				},
			},
		},
		{
			Name:           "handle SMALLINT",
			InputScanKind:  reflect.Struct,
			InputTypeName:  "SMALLINT",
			ConversionFunc: func(in *string) (*string, error) { return in, nil },
			Replacer: &sqlutil.StringFieldReplacer{
				OutputFieldType: data.FieldTypeNullableInt64,
				ReplaceFunc: func(in *string) (any, error) {
					if in == nil {
						return nil, nil
					}
					v, err := strconv.ParseInt(*in, 10, 64)
					if err != nil {
						return nil, err
					}
					return &v, nil
				},
			},
		},
		{
			Name:           "handle INT",
			InputScanKind:  reflect.Struct,
			InputTypeName:  "INT",
			ConversionFunc: func(in *string) (*string, error) { return in, nil },
			Replacer: &sqlutil.StringFieldReplacer{
				OutputFieldType: data.FieldTypeNullableInt64,
				ReplaceFunc: func(in *string) (any, error) {
					if in == nil {
						return nil, nil
					}
					v, err := strconv.ParseInt(*in, 10, 64)
					if err != nil {
						return nil, err
					}
					return &v, nil
				},
			},
		},
		{
			Name:           "handle FLOAT",
			InputScanKind:  reflect.Struct,
			InputTypeName:  "FLOAT",
			ConversionFunc: func(in *string) (*string, error) { return in, nil },
			Replacer: &sqlutil.StringFieldReplacer{
				OutputFieldType: data.FieldTypeNullableFloat64,
				ReplaceFunc: func(in *string) (any, error) {
					if in == nil {
						return nil, nil
					}
					v, err := strconv.ParseFloat(*in, 64)
					if err != nil {
						return nil, err
					}
					return &v, nil
				},
			},
		},
	}
}
