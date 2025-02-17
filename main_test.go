package main

import (
	"encoding/json"
	"testing"
)

func Test_LoadSensorData(t *testing.T) {
	payload := `{"time" : "2025-02-10 21:01:24", "model" : "LaCrosse-TX141THBv2", "id" : 245, "channel" : 0, "battery_ok" : 1, "temperature_C" : 9.000, "humidity" : 61, "test" : "No", "mic" : "CRC"}`
	var sensorData SensorData
	if err := json.Unmarshal([]byte(payload), &sensorData); err != nil {
		t.Errorf("Error unmarshalling sensor data: %v", err)
	}
	if sensorData.Id != 245 || sensorData.Channel != 0 || sensorData.BatteryOk != 1 || sensorData.TemperatureC != 9.0 || sensorData.Humidity != 61 {
		t.Errorf("Error unmarshalling sensor data: %v", sensorData)
	}
}

func Test_forMetrics(t *testing.T) {
	protoMetric := SensorData{
		Channel: 1,
		Id:      2,
	}
	ch := make(chan SensorData, 20)
	for v := range 10 {
		m := protoMetric
		m.TemperatureC = float64(v)
		ch <- m
	}
	close(ch)
	ct := 10
	for v := range ch {
		ct -= 1
		if v.Channel != 1 {
			t.Errorf("Expected channel 1, got %v", v.Channel)
		}
	}
	if ct != 0 {
		t.Errorf("Expected 0, got %v", ct)
	}
}
