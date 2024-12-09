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
	hdbDsn     = "hdb://HANA_READONLY:Hana@readonly456@172.18.3.30:30015"
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
				FieldType: data.FieldTypeFloat64,
				ConverterFunc: func(in interface{}) (interface{}, error) {
					rat, ok := in.(*driver.Decimal)
					if !ok {
						return nil, fmt.Errorf("value is not of type *big.Rat")
					}
					// Step 2: Convert *big.Rat to *string
					bigRat, err := rat.Value()
					if err != nil {
						return nil, fmt.Errorf("value is not of type *big.Rat")
					}

					rat1, ok := bigRat.(*big.Rat)
					if !ok {
						return nil, fmt.Errorf("decimal: invalid data type %T", bigRat)
					}
					f,_ := rat1.Float64()
					return f, nil
				},
			},
		},
	}
}
func GetConverterList() []sqlutil.StringConverter {
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
