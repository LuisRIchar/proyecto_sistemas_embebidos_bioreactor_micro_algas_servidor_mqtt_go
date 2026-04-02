package modelo

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/lib/pq" // Driver de PostgreSQL
)

var (
	ultimosDatos string
	datosMutex   sync.RWMutex
	db           *sql.DB
)

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	// Se actualiza en tiempo real para el Dashboard
	datosMutex.Lock()
	ultimosDatos = string(msg.Payload())
	datosMutex.Unlock()

	// Opcional: imprimir en consola para ver que sigue vivo
	// fmt.Printf("MQTT (en vivo): %s\n", ultimosDatos)
}

func initDB() {
	var err error
	// host=base-datos porque así se llama el servicio en tu docker-compose.yml
	connStr := "host=base-datos port=5432 user=admin password=contra12345 dbname=bioreactor_db sslmode=disable"

	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error crítico al iniciar driver de Postgres: %v", err)
	}

	// Esperamos un poco a que Postgres arranque por completo en Docker
	for i := 0; i < 5; i++ {
		err = db.Ping()
		if err == nil {
			break
		}
		log.Printf("Esperando a la base de datos... reintento en 3s")
		time.Sleep(3 * time.Second)
	}

	// Creamos la tabla automáticamente si es la primera vez que se ejecuta
	crearTablaSQL := `
	CREATE TABLE IF NOT EXISTS historico_sensores (
		id SERIAL PRIMARY KEY,
		fecha_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		luz REAL,
		temperatura REAL,
		turbidez REAL
	);`

	_, err = db.Exec(crearTablaSQL)
	if err != nil {
		log.Fatalf("Error al crear la tabla en Postgres: %v", err)
	}
	fmt.Println("Base de datos conectada y tabla verificada.")
}

func iniciarGuardadoPeriodico() {
	// Aquí defines cada cuánto tiempo quieres guardar el histórico
	ticker := time.NewTicker(1 * time.Minute)

	go func() {
		for {
			<-ticker.C // Espera a que pase 1 minuto
			guardarEnBD()
		}
	}()
}

func guardarEnBD() {
	datosMutex.RLock()
	datosActuales := ultimosDatos
	datosMutex.RUnlock()

	// Si el ESP32 aún no envía nada, no guardamos valores vacíos
	if datosActuales == "" {
		return
	}

	// Tu trama es "luz,temperatura,turbidez"
	partes := strings.Split(datosActuales, ",")
	if len(partes) != 3 {
		log.Printf("Trama ignorada por formato incorrecto: %s", datosActuales)
		return
	}

	luz, _ := strconv.ParseFloat(partes[0], 64)
	temp, _ := strconv.ParseFloat(partes[1], 64)
	turb, _ := strconv.ParseFloat(partes[2], 64)

	query := `INSERT INTO historico_sensores (luz, temperatura, turbidez) VALUES ($1, $2, $3)`
	_, err := db.Exec(query, luz, temp, turb)
	if err != nil {
		log.Printf("Error al insertar en la base de datos: %v", err)
	} else {
		fmt.Printf(">>> Histórico guardado: Luz=%.2f, Temp=%.2f, NTU=%.2f <<<\n", luz, temp, turb)
	}
}

func Init_mqtt() {
	// 1. Inicializar BD y crear tabla
	initDB()

	// 2. Arrancar el temporizador de 1 minuto en segundo plano
	iniciarGuardadoPeriodico()

	// 3. Configurar MQTT
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://servidor-mqtt:1883")
	opts.SetClientID("Servidor_go")
	opts.SetDefaultPublishHandler(messagePubHandler)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	client.Subscribe("bioreactor/sensores", 1, nil)

	// 4. Configurar Servidor HTTP
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "dashboard.html")
	})

	http.HandleFunc("/api/valores_crudos", func(w http.ResponseWriter, r *http.Request) {
		datosMutex.RLock()
		data := ultimosDatos
		datosMutex.RUnlock()
		w.Write([]byte(data))
	})

	fmt.Println("Web server running on http://192.168.100.100:9090")
	http.ListenAndServe(":9090", nil)
}
