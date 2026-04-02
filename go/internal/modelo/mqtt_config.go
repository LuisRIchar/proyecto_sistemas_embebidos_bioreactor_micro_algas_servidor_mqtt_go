package modelo

import (
	"fmt"
	"net/http"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	ultimosDatos string
	datosMutex   sync.RWMutex
)

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	// Bloqueamos para escritura segura
	datosMutex.Lock()
	ultimosDatos = string(msg.Payload())
	datosMutex.Unlock()

	fmt.Printf("Valores recibidos: %s\n", string(msg.Payload()))
}

func Init_mqtt() {

	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://servidor-mqtt:1883")
	opts.SetClientID("Servidor_go")
	opts.SetDefaultPublishHandler(messagePubHandler)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	client.Subscribe("bioreactor/sensores", 1, nil)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "dashboard.html")
	})

	http.HandleFunc("/api/valores_crudos", func(w http.ResponseWriter, r *http.Request) {
		// Bloqueamos solo para lectura segura
		datosMutex.RLock()
		data := ultimosDatos
		datosMutex.RUnlock()

		w.Write([]byte(data))
	})

	fmt.Println("Web server running on http://192.168.100.100:9090")
	http.ListenAndServe(":9090", nil)
}
