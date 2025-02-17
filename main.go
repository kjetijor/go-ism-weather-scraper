package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/shlex"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

// echo '{"time" : "2025-02-10 21:01:24", "model" : "LaCrosse-TX141THBv2", "id" : 245, "channel" : 0, "battery_ok" : 1, "temperature_C" : 9.000, "humidity" : 61, "test" : "No", "mic" : "CRC"}'
type SensorData struct {
	TimeString   string  `json:"time"`
	Model        string  `json:"model"`
	Id           int     `json:"id"`
	Channel      int     `json:"channel"`
	BatteryOk    int     `json:"battery_ok"`
	TemperatureC float64 `json:"temperature_C"`
	Humidity     int     `json:"humidity"`
	Test         string  `json:"test"`
	Mic          string  `json:"mic"`
}

type MetricsConfig struct {
	NodeExporterDir string
	FilenameStub    string
	Labels          map[string]string
	StaleInterval   time.Duration
}

func producer(wg *sync.WaitGroup, scanner *bufio.Scanner, sensorPipe chan<- SensorData) {
	defer wg.Done()
	for scanner.Scan() {
		payload := scanner.Text()
		var sensorData SensorData
		if err := json.Unmarshal([]byte(payload), &sensorData); err != nil {
			log.Printf("Error unmarshalling sensor data: %v", err)
		}
		sensorPipe <- sensorData
		if err := scanner.Err(); err != nil {
			log.Printf("Error reading from scanner: %v", err)
			break
		}
	}
}

func sweeper(ctx context.Context, wg *sync.WaitGroup, dir, stub string, staleAge time.Duration) {
	defer wg.Done()
	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			entries, err := os.ReadDir(dir)
			if err != nil {
				log.Printf("Error reading directory %s: %v", dir, err)
				continue
			}
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				if !strings.HasPrefix(entry.Name(), stub) {
					continue
				}
				if !(strings.HasSuffix(entry.Name(), ".prom") || strings.HasSuffix(entry.Name(), ".prom.tmp")) {
					continue
				}
				stat, err := entry.Info()
				if err != nil {
					log.Printf("Error getting file info for %s: %v", entry.Name(), err)
					continue
				}
				if time.Since(stat.ModTime()) > staleAge {
					// FIXME: inspect the file also...
					log.Printf("sweeping stale file %s", entry.Name())
					if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
						log.Printf("Error removing file %s: %v", entry.Name(), err)
					}
				}
			}
		}
	}
}

func writeMetrics(cfg MetricsConfig, sensorData SensorData) error {
	labelNames := []string{"id", "channel"}
	labelValues := []string{fmt.Sprintf("%d", sensorData.Id), fmt.Sprintf("%d", sensorData.Channel)}
	for k, v := range cfg.Labels {
		labelNames = append(labelNames, k)
		labelValues = append(labelValues, v)
	}
	reg := prometheus.NewRegistry()
	tempC := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "temperature_C",
		Help: "Temperature in Celsius",
	}, labelNames)
	humidity := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "relative_humidity_pct",
		Help: "Relative humidity in percent",
	}, labelNames)
	reg.MustRegister(tempC)
	reg.MustRegister(humidity)
	tempC.WithLabelValues(labelValues...).Set(sensorData.TemperatureC)
	humidity.WithLabelValues(labelValues...).Set(float64(sensorData.Humidity))

	metrics, err := reg.Gather()
	if err != nil {
		return fmt.Errorf("error gathering metrics: %v", err)
	}

	outfn := filepath.Join(cfg.NodeExporterDir, fmt.Sprintf("%s_%d_%d.prom", cfg.FilenameStub, sensorData.Id, sensorData.Channel))
	tmpfn := outfn + ".tmp"

	fh, err := os.Create(tmpfn)
	if err != nil {
		return fmt.Errorf("error creating file %s: %v", tmpfn, err)
	}
	defer fh.Close()
	defer func() {
		if err := os.Remove(tmpfn); err != nil && !os.IsNotExist(err) {
			log.Printf("Error removing temporary file %s: %v", tmpfn, err)
		}
	}()

	for _, metric := range metrics {
		if _, err := expfmt.MetricFamilyToText(fh, metric); err != nil {
			return fmt.Errorf("error writing metric: %v", err)
		}
	}

	return os.Rename(tmpfn, outfn)
}

func metricsWriter(wg *sync.WaitGroup, cfg MetricsConfig, sensorPipe <-chan SensorData) {
	defer wg.Done()
	for sensorData := range sensorPipe {
		if err := writeMetrics(cfg, sensorData); err != nil {
			log.Printf("Error writing metrics: %v", err)
			continue
		}
	}
}

func splitLabels(labels string) map[string]string {
	labelMap := make(map[string]string)
	if labels == "" {
		return labelMap
	}
	for _, label := range strings.Split(labels, ",") {
		parts := strings.Split(label, "=")
		if len(parts) != 2 {
			log.Fatalf("Error parsing label %s", label)
		}
		labelMap[parts[0]] = parts[1]
	}
	return labelMap
}

func main() {
	cfg := MetricsConfig{}
	flag.StringVar(&cfg.NodeExporterDir, "node-exporter-dir", "/var/lib/prometheus/node-exporter", "Node exporter directory")
	flag.StringVar(&cfg.FilenameStub, "filename", "rtl433", "Output filename stub")
	flag.DurationVar(&cfg.StaleInterval, "stale-interval", 15*time.Minute, "Stale interval")
	labelString := flag.String("labels", "", "Labels to add to metrics")
	rtl433cmd := flag.String("cmd", "rtl_433 -R 73 -F json -T 3600", "Command to run")
	flag.Parse()

	cfg.Labels = splitLabels(*labelString)

	cmdSpec, err := shlex.Split(*rtl433cmd)
	if err != nil {
		log.Fatalf("Error parsing command: %v", err)
	}
	log.Printf("Scraping '%s' into '%s' with labels %s", *rtl433cmd, cfg.NodeExporterDir, *labelString)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cmd := exec.CommandContext(ctx, cmdSpec[0], cmdSpec[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("Error getting stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatalf("Error starting command: %v", err)
	}

	metricsPipe := make(chan SensorData)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go producer(wg, bufio.NewScanner(stdout), metricsPipe)
	wg.Add(1)
	go sweeper(ctx, wg, cfg.NodeExporterDir, cfg.FilenameStub, cfg.StaleInterval)
	wg.Add(1)
	go metricsWriter(wg, cfg, metricsPipe)

	go func() {
		<-ctx.Done()
		log.Printf("Received signal, shutting down metrics pipe")
		close(metricsPipe)
	}()
	log.Printf("Startup complete, waiting for signal")
	wg.Wait()
	if err := cmd.Process.Kill(); err != nil {
		log.Printf("Error killing command: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
		if !(ok && ws.Signaled() && (ws.Signal() == syscall.SIGTERM || ws.Signal() == syscall.SIGINT)) {
			log.Printf("Error waiting for command: %v", err)
		}
	}
}
