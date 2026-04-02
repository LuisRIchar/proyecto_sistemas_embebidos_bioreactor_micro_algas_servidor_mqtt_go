/*
 * Copyright 2026 Luis Ricardo Serrano Dzib
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package modelo

import (
	"fmt"
	"net/http"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var ultimosDatos string

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	ultimosDatos = string(msg.Payload())
	fmt.Printf("Valores recibidos: %s\n", ultimosDatos)
}

func Init_mqtt() {

	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://mqtt-server:1833")
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
		w.Write([]byte(ultimosDatos))
	})

	fmt.Println("Web server running on http://192.168.100.100:9090")
	http.ListenAndServe(":9090", nil)
}
