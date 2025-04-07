package sqleng

import (
	"database/sql"
	"fmt"
	"log"
	"math/big"
	"reflect"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/SAP/go-hdb/driver"
	_ "github.com/SAP/go-hdb/driver"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
)

const (
	dateFormat      = "2006-01-02"
	dateTimeFormat1 = "2006-01-02 15:04:05"
	dateTimeFormat2 = "2006-01-02T15:04:05Z"
)
const (
	driverName = "hdb"
	hdbDsn     = "hdb://HANA_READONLY:Hana@readonly456@192.168.32.182:30015"
)

func GetConverterList2() []sqlutil.Converter {

	var fixedPattern = regexp.MustCompile(`FIXED\d{1,2}`)
	return []sqlutil.Converter{
		{
			Name: "handle DECIMAL",
			// InputScanKind:  reflect.Slice,
			// InputTypeName: "FIXED8",
			InputTypeRegex: fixedPattern,
			FrameConverter: sqlutil.FrameConverter{
				FieldType: data.FieldTypeNullableFloat64,
				ConverterFunc: func(in interface{}) (interface{}, error) {
					switch x := in.(type) {
					case *driver.NullDecimal:
						if x.Valid {
							d := (*big.Rat)(x.Decimal)
							f, _ := d.Float64()
							return &f, nil
						} else {
							return nil, nil
						}

					case *driver.Decimal:
						{
							bigRat, err := x.Value()
							if err != nil {
								return nil, fmt.Errorf("value is not of type *big.Rat")
							}
							rat1, ok := bigRat.(*big.Rat)
							if !ok {
								return nil, fmt.Errorf("decimal: invalid data type %T", bigRat)
							}
							f, _ := rat1.Float64()
							return &f, nil
						}
					}
					return nil, fmt.Errorf("decimal: invalid data value")
				},
			},
		},
	}
}
func GetConverterList() []sqlutil.StringConverter {
	// For the HANA driver , we have these possible data types:
	// https://help.sap.com/docs/HANA_SERVICE_CF/7c78579ce9b14a669c1f3295b0d8ca16/20a1569875191014b507cf392724b7eb.html.
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

func TestDataSourceHandler_executeQuery(t *testing.T) {

	db, err := sql.Open(driverName, hdbDsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("select LABST from saphanadb.mard limit 1")
	if err != nil {
		log.Fatal(err)
		return
	}
	// stringConverters := GetConverterList()

	// converts:=sqlutil.ToConverters(stringConverters...);

	converts2 := GetConverterList2()

	// converts = append(converts, converts2...)

	_, err = sqlutil.FrameFromRows(rows, 100, converts2...)
	if err != nil {
		log.Fatal(err)
		return
	}
}

func TestDataSourceHandler2_executeQuery(t *testing.T) {

	db, err := sql.Open(driverName, hdbDsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("select DMBTR from saphanadb.bseg limit 1")
	if err != nil {
		log.Fatal(err)
		return
	}
	// stringConverters := GetConverterList()

	// converts:=sqlutil.ToConverters(stringConverters...);

	converts2 := GetConverterList2()

	// converts = append(converts, converts2...)

	_, err = sqlutil.FrameFromRows(rows, 100, converts2...)
	if err != nil {
		log.Fatal(err)
		return
	}
}

func TestDataSourceHandler3_executeQuery(t *testing.T) {

	db, err := sql.Open(driverName, hdbDsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("select DMB31 from saphanadb.bseg limit 1")
	if err != nil {
		log.Fatal(err)
		return
	}
	// stringConverters := GetConverterList()

	// converts:=sqlutil.ToConverters(stringConverters...);

	converts2 := GetConverterList2()

	// converts = append(converts, converts2...)

	_, err = sqlutil.FrameFromRows(rows, 100, converts2...)
	if err != nil {
		log.Fatal(err)
		return
	}
}

func TestDataSourceHandler4_executeQuery(t *testing.T) {

	db, err := sql.Open(driverName, hdbDsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	sql := `SELECT 
  SNAPSHOT_TIME,
  HOST,
  TO_DECIMAL(DATA_GB, 10, 2) as DATA_GB,
  TO_DECIMAL(LOG_GB, 10, 2) as LOG_GB,
  TO_DECIMAL(DATA_BACKUP_GB, 10, 2) as DATA_BKP_GB,
  TO_DECIMAL(LOG_BACKUP_GB, 10, 2) as LOG_BKP_GB,
  TO_DECIMAL(CATALOG_BACKUP_GB, 10, 2) as CAT_BKP_GB,
  TO_DECIMAL(TRACE_GB, 10, 2) as TRACE_GB,
  TO_DECIMAL(ROOTKEY_BACKUP_GB, 10, 2) as RK_BKP_GB
FROM
( SELECT
    SNAPSHOT_TIME,
    CASE WHEN AGGREGATE_BY = 'NONE' OR INSTR(AGGREGATE_BY, 'HOST') != 0 THEN HOST ELSE MAP(BI_HOST, '%', 'any', BI_HOST) END HOST,
    SUM(DATA_GB) DATA_GB,
    SUM(LOG_GB) LOG_GB,
    SUM(DATA_BACKUP_GB) DATA_BACKUP_GB,
    SUM(LOG_BACKUP_GB) LOG_BACKUP_GB,
    SUM(CATALOG_BACKUP_GB) CATALOG_BACKUP_GB,
    SUM(TRACE_GB) TRACE_GB,
    SUM(ROOTKEY_BACKUP_GB) ROOTKEY_BACKUP_GB
  FROM
  ( SELECT
      CASE 
        WHEN BI.AGGREGATE_BY = 'NONE' OR INSTR(BI.AGGREGATE_BY, 'TIME') != 0 THEN 
        CASE 
          WHEN BI.TIME_AGGREGATE_BY LIKE 'TS%' THEN
            TO_VARCHAR(ADD_SECONDS(TO_TIMESTAMP('2014/01/01 00:00:00', 'YYYY/MM/DD HH24:MI:SS'), FLOOR(SECONDS_BETWEEN(TO_TIMESTAMP('2014/01/01 00:00:00', 
            'YYYY/MM/DD HH24:MI:SS'), CASE BI.TIMEZONE WHEN 'UTC' THEN ADD_SECONDS(DU.SNAPSHOT_TIME, SECONDS_BETWEEN(CURRENT_TIMESTAMP, CURRENT_UTCTIMESTAMP)) ELSE DU.SNAPSHOT_TIME END) / SUBSTR(BI.TIME_AGGREGATE_BY, 3)) * SUBSTR(BI.TIME_AGGREGATE_BY, 3)), 'YYYY/MM/DD HH24:MI:SS')
          ELSE TO_VARCHAR(CASE BI.TIMEZONE WHEN 'UTC' THEN ADD_SECONDS(DU.SNAPSHOT_TIME, SECONDS_BETWEEN(CURRENT_TIMESTAMP, CURRENT_UTCTIMESTAMP)) ELSE DU.SNAPSHOT_TIME END, BI.TIME_AGGREGATE_BY)
        END
        ELSE 'any'
      END SNAPSHOT_TIME,
      DU.HOST,
      AVG(DU.DATA) / 1024 / 1024 / 1024 DATA_GB,
      AVG(DU.LOG) / 1024 / 1024 / 1024 LOG_GB,
      AVG(DU.DATA_BACKUP) / 1024 / 1024 / 1024 DATA_BACKUP_GB,
      AVG(DU.LOG_BACKUP) / 1024 / 1024 / 1024 LOG_BACKUP_GB,
      AVG(DU.CATALOG_BACKUP) / 1024 / 1024 / 1024 CATALOG_BACKUP_GB,
      AVG(DU.TRACE) / 1024 / 1024 / 1024 TRACE_GB,
      AVG(DU.ROOTKEY_BACKUP) / 1024 / 1024 / 1024 ROOTKEY_BACKUP_GB,
      BI.AGGREGATE_BY,
      BI.HOST BI_HOST
    FROM
    ( SELECT
        CASE
          WHEN BEGIN_TIME =    'C'                             THEN CURRENT_TIMESTAMP
          WHEN BEGIN_TIME LIKE 'C-S%'                          THEN ADD_SECONDS(CURRENT_TIMESTAMP, -SUBSTR_AFTER(BEGIN_TIME, 'C-S'))
          WHEN BEGIN_TIME LIKE 'C-M%'                          THEN ADD_SECONDS(CURRENT_TIMESTAMP, -SUBSTR_AFTER(BEGIN_TIME, 'C-M') * 60)
          WHEN BEGIN_TIME LIKE 'C-H%'                          THEN ADD_SECONDS(CURRENT_TIMESTAMP, -SUBSTR_AFTER(BEGIN_TIME, 'C-H') * 3600)
          WHEN BEGIN_TIME LIKE 'C-D%'                          THEN ADD_SECONDS(CURRENT_TIMESTAMP, -SUBSTR_AFTER(BEGIN_TIME, 'C-D') * 86400)
          WHEN BEGIN_TIME LIKE 'C-W%'                          THEN ADD_SECONDS(CURRENT_TIMESTAMP, -SUBSTR_AFTER(BEGIN_TIME, 'C-W') * 86400 * 7)
          WHEN BEGIN_TIME LIKE 'E-S%'                          THEN ADD_SECONDS(TO_TIMESTAMP(END_TIME, 'YYYY/MM/DD HH24:MI:SS'), -SUBSTR_AFTER(BEGIN_TIME, 'E-S'))
          WHEN BEGIN_TIME LIKE 'E-M%'                          THEN ADD_SECONDS(TO_TIMESTAMP(END_TIME, 'YYYY/MM/DD HH24:MI:SS'), -SUBSTR_AFTER(BEGIN_TIME, 'E-M') * 60)
          WHEN BEGIN_TIME LIKE 'E-H%'                          THEN ADD_SECONDS(TO_TIMESTAMP(END_TIME, 'YYYY/MM/DD HH24:MI:SS'), -SUBSTR_AFTER(BEGIN_TIME, 'E-H') * 3600)
          WHEN BEGIN_TIME LIKE 'E-D%'                          THEN ADD_SECONDS(TO_TIMESTAMP(END_TIME, 'YYYY/MM/DD HH24:MI:SS'), -SUBSTR_AFTER(BEGIN_TIME, 'E-D') * 86400)
          WHEN BEGIN_TIME LIKE 'E-W%'                          THEN ADD_SECONDS(TO_TIMESTAMP(END_TIME, 'YYYY/MM/DD HH24:MI:SS'), -SUBSTR_AFTER(BEGIN_TIME, 'E-W') * 86400 * 7)
          WHEN BEGIN_TIME =    'MIN'                           THEN TO_TIMESTAMP('1000/01/01 00:00:00', 'YYYY/MM/DD HH24:MI:SS')
          WHEN SUBSTR(BEGIN_TIME, 1, 1) NOT IN ('C', 'E', 'M') THEN TO_TIMESTAMP(BEGIN_TIME, 'YYYY/MM/DD HH24:MI:SS')
        END BEGIN_TIME,
        CASE
          WHEN END_TIME =    'C'                             THEN CURRENT_TIMESTAMP
          WHEN END_TIME LIKE 'C-S%'                          THEN ADD_SECONDS(CURRENT_TIMESTAMP, -SUBSTR_AFTER(END_TIME, 'C-S'))
          WHEN END_TIME LIKE 'C-M%'                          THEN ADD_SECONDS(CURRENT_TIMESTAMP, -SUBSTR_AFTER(END_TIME, 'C-M') * 60)
          WHEN END_TIME LIKE 'C-H%'                          THEN ADD_SECONDS(CURRENT_TIMESTAMP, -SUBSTR_AFTER(END_TIME, 'C-H') * 3600)
          WHEN END_TIME LIKE 'C-D%'                          THEN ADD_SECONDS(CURRENT_TIMESTAMP, -SUBSTR_AFTER(END_TIME, 'C-D') * 86400)
          WHEN END_TIME LIKE 'C-W%'                          THEN ADD_SECONDS(CURRENT_TIMESTAMP, -SUBSTR_AFTER(END_TIME, 'C-W') * 86400 * 7)
          WHEN END_TIME LIKE 'B+S%'                          THEN ADD_SECONDS(TO_TIMESTAMP(BEGIN_TIME, 'YYYY/MM/DD HH24:MI:SS'), SUBSTR_AFTER(END_TIME, 'B+S'))
          WHEN END_TIME LIKE 'B+M%'                          THEN ADD_SECONDS(TO_TIMESTAMP(BEGIN_TIME, 'YYYY/MM/DD HH24:MI:SS'), SUBSTR_AFTER(END_TIME, 'B+M') * 60)
          WHEN END_TIME LIKE 'B+H%'                          THEN ADD_SECONDS(TO_TIMESTAMP(BEGIN_TIME, 'YYYY/MM/DD HH24:MI:SS'), SUBSTR_AFTER(END_TIME, 'B+H') * 3600)
          WHEN END_TIME LIKE 'B+D%'                          THEN ADD_SECONDS(TO_TIMESTAMP(BEGIN_TIME, 'YYYY/MM/DD HH24:MI:SS'), SUBSTR_AFTER(END_TIME, 'B+D') * 86400)
          WHEN END_TIME LIKE 'B+W%'                          THEN ADD_SECONDS(TO_TIMESTAMP(BEGIN_TIME, 'YYYY/MM/DD HH24:MI:SS'), SUBSTR_AFTER(END_TIME, 'B+W') * 86400 * 7)
          WHEN END_TIME =    'MAX'                           THEN TO_TIMESTAMP('9999/12/31 00:00:00', 'YYYY/MM/DD HH24:MI:SS')
          WHEN SUBSTR(END_TIME, 1, 1) NOT IN ('C', 'B', 'M') THEN TO_TIMESTAMP(END_TIME, 'YYYY/MM/DD HH24:MI:SS')
        END END_TIME,
        TIMEZONE,
        HOST,
        DATA_SOURCE,
        AGGREGATE_BY,
        MAP(TIME_AGGREGATE_BY,
          'NONE',        'YYYY/MM/DD HH24:MI:SS:FF7',
          'HOUR',        'YYYY/MM/DD HH24',
          'DAY',         'YYYY/MM/DD (DY)',
          'HOUR_OF_DAY', 'HH24',
          TIME_AGGREGATE_BY ) TIME_AGGREGATE_BY
      FROM
      ( SELECT                       /* Modification section */
          '1000/10/18 07:58:00' BEGIN_TIME,                  /* YYYY/MM/DD HH24:MI:SS timestamp, C, C-S<seconds>, C-M<minutes>, C-H<hours>, C-D<days>, C-W<weeks>, E-S<seconds>, E-M<minutes>, E-H<hours>, E-D<days>, E-W<weeks>, MIN */
          '9999/10/18 08:05:00' END_TIME,                    /* YYYY/MM/DD HH24:MI:SS timestamp, C, C-S<seconds>, C-M<minutes>, C-H<hours>, C-D<days>, C-W<weeks>, B+S<seconds>, B+M<minutes>, B+H<hours>, B+D<days>, B+W<weeks>, MAX */
          'SERVER' TIMEZONE,                              /* SERVER, UTC */
          '%' HOST,
          'HISTORY' DATA_SOURCE,               /* CURRENT, HISTORY */
          'NONE' AGGREGATE_BY,           /* TIME, HOST or comma separated combinations, NONE for no aggregation */
          'DAY' TIME_AGGREGATE_BY     /* HOUR, DAY, HOUR_OF_DAY or database time pattern, TS<seconds> for time slice, NONE for no aggregation */
        FROM
          DUMMY
      )
    ) BI,
    ( SELECT
        'CURRENT' DATA_SOURCE,
        CURRENT_TIMESTAMP SNAPSHOT_TIME,
        HOST,
        SUM(MAP(USAGE_TYPE, 'DATA', USED_SIZE, 0)) DATA,
        SUM(MAP(USAGE_TYPE, 'LOG', USED_SIZE, 0)) LOG,
        SUM(MAP(USAGE_TYPE, 'DATA_BACKUP', USED_SIZE, 0)) DATA_BACKUP,
        SUM(MAP(USAGE_TYPE, 'LOG_BACKUP', USED_SIZE, 0)) LOG_BACKUP,
        SUM(MAP(USAGE_TYPE, 'TRACE', USED_SIZE, 0)) TRACE,
        SUM(MAP(USAGE_TYPE, 'CATALOG_BACKUP', USED_SIZE, 0)) CATALOG_BACKUP,
        SUM(MAP(USAGE_TYPE, 'ROOTKEY_BACKUP', USED_SIZE, 0)) ROOTKEY_BACKUP
      FROM
        M_DISK_USAGE
      GROUP BY
        HOST
      UNION ALL
      SELECT
        'HISTORY' DATA_SOURCE,
        SERVER_TIMESTAMP SNAPSHOT_TIME,
        HOST,
        SUM(MAP(USAGE_TYPE, 'DATA', USED_SIZE, 0)) DATA,
        SUM(MAP(USAGE_TYPE, 'LOG', USED_SIZE, 0)) LOG,
        SUM(MAP(USAGE_TYPE, 'DATA_BACKUP', USED_SIZE, 0)) DATA_BACKUP,
        SUM(MAP(USAGE_TYPE, 'LOG_BACKUP', USED_SIZE, 0)) LOG_BACKUP,
        SUM(MAP(USAGE_TYPE, 'TRACE', USED_SIZE, 0)) TRACE,
        SUM(MAP(USAGE_TYPE, 'CATALOG_BACKUP', USED_SIZE, 0)) CATALOG_BACKUP,
        SUM(MAP(USAGE_TYPE, 'ROOTKEY_BACKUP', USED_SIZE, 0)) ROOTKEY_BACKUP
      FROM
        _SYS_STATISTICS.GLOBAL_DISK_USAGE
      GROUP BY
        SERVER_TIMESTAMP,
        HOST
    ) DU
    WHERE
      CASE BI.TIMEZONE WHEN 'UTC' THEN ADD_SECONDS(DU.SNAPSHOT_TIME, SECONDS_BETWEEN(CURRENT_TIMESTAMP, CURRENT_UTCTIMESTAMP)) ELSE DU.SNAPSHOT_TIME END BETWEEN BI.BEGIN_TIME AND BI.END_TIME AND
      DU.HOST LIKE BI.HOST AND
      DU.DATA_SOURCE = BI.DATA_SOURCE
    GROUP BY
      CASE 
        WHEN BI.AGGREGATE_BY = 'NONE' OR INSTR(BI.AGGREGATE_BY, 'TIME') != 0 THEN 
        CASE 
          WHEN BI.TIME_AGGREGATE_BY LIKE 'TS%' THEN
            TO_VARCHAR(ADD_SECONDS(TO_TIMESTAMP('2014/01/01 00:00:00', 'YYYY/MM/DD HH24:MI:SS'), FLOOR(SECONDS_BETWEEN(TO_TIMESTAMP('2014/01/01 00:00:00', 
            'YYYY/MM/DD HH24:MI:SS'), CASE BI.TIMEZONE WHEN 'UTC' THEN ADD_SECONDS(DU.SNAPSHOT_TIME, SECONDS_BETWEEN(CURRENT_TIMESTAMP, CURRENT_UTCTIMESTAMP)) ELSE DU.SNAPSHOT_TIME END) / SUBSTR(BI.TIME_AGGREGATE_BY, 3)) * SUBSTR(BI.TIME_AGGREGATE_BY, 3)), 'YYYY/MM/DD HH24:MI:SS')
          ELSE TO_VARCHAR(CASE BI.TIMEZONE WHEN 'UTC' THEN ADD_SECONDS(DU.SNAPSHOT_TIME, SECONDS_BETWEEN(CURRENT_TIMESTAMP, CURRENT_UTCTIMESTAMP)) ELSE DU.SNAPSHOT_TIME END, BI.TIME_AGGREGATE_BY)
        END
        ELSE 'any'
      END,
      DU.HOST,
      BI.AGGREGATE_BY,
      BI.HOST
  )
  GROUP BY
    SNAPSHOT_TIME,
    CASE WHEN AGGREGATE_BY = 'NONE' OR INSTR(AGGREGATE_BY, 'HOST') != 0 THEN HOST ELSE MAP(BI_HOST, '%', 'any', BI_HOST) END
)
ORDER BY
  SNAPSHOT_TIME DESC,
  HOST
`
	rows, err := db.Query(sql)
	if err != nil {
		log.Fatal(err)
		return
	}
	// stringConverters := GetConverterList()

	// converts:=sqlutil.ToConverters(stringConverters...);

	converts2 := GetConverterList2()

	// converts = append(converts, converts2...)

	_, err = sqlutil.FrameFromRows(rows, 100, converts2...)
	if err != nil {
		log.Fatal(err)
		return
	}
}
