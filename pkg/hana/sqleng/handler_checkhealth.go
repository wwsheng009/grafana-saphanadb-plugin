package sqleng

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/SAP/go-hdb/driver"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

func (e *DataSourceHandler) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	err := e.db.Ping()
	if err != nil {
		logCheckHealthError(ctx, e.dsInfo, err, e.log)
		if strings.EqualFold(req.PluginContext.User.Role, "Admin") {
			return ErrToHealthCheckResult(err)
		}
		var driverErr driver.Error
		if errors.As(err, &driverErr) {
			return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: e.TransformQueryError(e.log, err).Error()}, nil
		}
		return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: e.TransformQueryError(e.log, err).Error()}, nil
	}
	return &backend.CheckHealthResult{Status: backend.HealthStatusOk, Message: "Database Connection OK"}, nil
}

// ErrToHealthCheckResult converts error into user friendly health check message
// This should be called with non nil error. If the err parameter is empty, we will send Internal Server Error
func ErrToHealthCheckResult(err error) (*backend.CheckHealthResult, error) {
	if err == nil {
		return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: "Internal Server Error"}, nil
	}
	res := &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: err.Error()}
	details := map[string]string{}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		res.Message = "Network error: Failed to connect to the server"
		if opErr != nil && opErr.Err != nil {
			res.Message += fmt.Sprintf(". Error message: %s", opErr.Err.Error())
		}
		details["verboseMessage"] = err.Error()
		details["errorDetailsLink"] = "https://grafana.com/docs/grafana/latest/datasources/mysql/#configure-the-data-source"
	}
	var driverErr driver.Error
	if errors.As(err, &driverErr) {
		res.Message = "Database error: Failed to connect to the SAP HANA server"
		if driverErr != nil && driverErr.Code() > 0 {
			res.Message += fmt.Sprintf(". HANA error number: %d", driverErr.Code())
		}
		details["verboseMessage"] = err.Error()
		details["errorDetailsLink"] = "https://help.sap.com/docs/HANA_SERVICE_CF/7c78579ce9b14a669c1f3295b0d8ca16/20a78d3275191014b41bae7c4a46d835.html"
	}
	detailBytes, marshalErr := json.Marshal(details)
	if marshalErr != nil {
		return res, nil
	}
	res.JSONDetails = detailBytes
	return res, nil
}

func logCheckHealthError(_ context.Context, dsInfo DataSourceInfo, err error, logger log.Logger) {
	configSummary := map[string]any{
		"config_url_length":                 len(dsInfo.URL),
		"config_user_length":                len(dsInfo.User),
		"config_database_length":            len(dsInfo.Database),
		"config_json_data_database_length":  len(dsInfo.JsonData.Database),
		"config_max_open_conns":             dsInfo.JsonData.MaxOpenConns,
		"config_max_idle_conns":             dsInfo.JsonData.MaxIdleConns,
		"config_conn_max_life_time":         dsInfo.JsonData.ConnMaxLifetime,
		"config_conn_timeout":               dsInfo.JsonData.ConnectionTimeout,
		"config_timescaledb":                dsInfo.JsonData.Timescaledb,
		"config_ssl_mode":                   dsInfo.JsonData.Mode,
		"config_tls_configuration_method":   dsInfo.JsonData.ConfigurationMethod,
		"config_tls_skip_verify":            dsInfo.JsonData.TlsSkipVerify,
		"config_timezone":                   dsInfo.JsonData.Timezone,
		"config_time_interval":              dsInfo.JsonData.TimeInterval,
		"config_enable_secure_proxy":        dsInfo.JsonData.SecureDSProxy,
		"config_allow_clear_text_passwords": dsInfo.JsonData.AllowCleartextPasswords,
		"config_authentication_type":        dsInfo.JsonData.AuthenticationType,
		"config_ssl_root_cert_file_length":  len(dsInfo.JsonData.RootCertFile),
		"config_ssl_cert_file_length":       len(dsInfo.JsonData.CertFile),
		"config_ssl_key_file_length":        len(dsInfo.JsonData.CertKeyFile),
		"config_encrypt_length":             len(dsInfo.JsonData.Encrypt),
		"config_server_name_length":         len(dsInfo.JsonData.Servername),
		"config_password_length":            len(dsInfo.DecryptedSecureJSONData["password"]),
		"config_tls_ca_cert_length":         len(dsInfo.DecryptedSecureJSONData["tlsCACert"]),
		"config_tls_client_cert_length":     len(dsInfo.DecryptedSecureJSONData["tlsClientCert"]),
		"config_tls_client_key_length":      len(dsInfo.DecryptedSecureJSONData["tlsClientKey"]),
	}
	configSummaryJson, marshalError := json.Marshal(configSummary)
	if marshalError != nil {
		logger.Error("Check health failed", "error", err, "message_type", "ds_config_health_check_error", "plugin_id", "hdb")
		return
	}
	logger.Error("Check health failed", "error", err, "message_type", "ds_config_health_check_error_detailed", "plugin_id", "hdb", "details", string(configSummaryJson))
}
