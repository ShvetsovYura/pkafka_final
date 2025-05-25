package main

func main() {
	// data, err := os.ReadFile("config.yml")
	// if err != nil {
	// 	log.Fatalf("Failed to read config file: %v", err)
	// }

	// var cfg types.AndlyticsAppConfig
	// if err := yaml.Unmarshal(data, &cfg); err != nil {
	// 	log.Fatalf("Failed to parse config: %v", err)
	// }
	// hc, err := analytics.NewHDFSClient(cfg.HDFS)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// // hc.MakeDirs()
	// err = hc.Write("super_user_id", "телевизор цветной ламповый")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// err = analytics.RunCalc(context.Background(), "sc://localhost:15002", "hdfs://localhost:9000/data/super_user_id/jgogo', nil)
	// if err != nil {
	// 	log.Fatal("error on calc %s", err)
	// }
}
