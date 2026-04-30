// Servico: se conecta por TCP ao Broker e envia dados continuamente (a cada 1s)

// -------- Struct --------
// Envia dados aleatórios no intervalo de 0 a 100

// servico: se conecta por TCP ao Broker e envia dados continuamente (a cada 1s)

// -------- Struct --------
// Mensagem que contém Tipo, ID, TimeStamp, Valor

package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"time"
)

// -------- Struct --------

type MensagemServico struct {
	Tipo       string  `json:"tipo"`
	ID         string  `json:"id"`
	Timestamp  int64   `json:"timestamp"`
	Prioridade int64  `json:"prioridade"`
}

// -------- Funções ----------

func conectarBroker(addr string, msg MensagemServico) net.Conn {
	for {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Erro ao conectar ao broker:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// Mensagem para indicar primeira conexão
		enviarMensagem(conn, msg)
		fmt.Println("Conectado ao broker:", addr)
		return conn
	}
}

func enviarMensagem(conn net.Conn, msg MensagemServico) {
	data, _ := json.Marshal(msg)
	conn.Write(append(data, '\n'))
}

func gerarValor() int64 {
	return int64 (rand.Intn(8))
}

// -------- Main ----------

func main() {

	rand.Seed(time.Now().UnixNano())

	id := fmt.Sprintf("servico-%d", rand.Intn(1000))

	// Mensagem Base do Servico em execução
	msg := MensagemServico{
			Tipo:      "servico",
			ID:        id,
	}

	brokerAddr := "localhost:8001"
	conn := conectarBroker(brokerAddr, msg)
	defer conn.Close()


	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		msg.Timestamp = time.Now().UnixNano()
		msg.Prioridade = gerarValor()

		enviarMensagem(conn, msg)

		fmt.Printf("[%s] Valor enviado: %d\n", id, msg.Prioridade)
	}
}