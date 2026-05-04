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
	Tipo       string `json:"tipo"`
	ID         string `json:"id"`
	Timestamp  int64  `json:"timestamp"`
	Prioridade int64  `json:"prioridade"`
}

// -------- Utils --------

func timeStamp() string {
	t := time.Now()
	return fmt.Sprintf("%02d:%02d:%02d",
		t.Hour(),
		t.Minute(),
		t.Second())
}

// -------- Funções ----------

func conectarBroker(addr string, msg MensagemServico) net.Conn {
	for {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("(%s) [Servico %s] - [CONN]: erro ao conectar %s\n", timeStamp(), msg.ID, addr)
			time.Sleep(2 * time.Second)
			continue
		}

		fmt.Printf("(%s) [Servico %s] - [CONN]: conectado ao broker %s\n", timeStamp(), msg.ID, addr)

		// handshake inicial
		enviarMensagem(conn, msg)

		return conn
	}
}

func enviarMensagem(conn net.Conn, msg MensagemServico) error {
	data, _ := json.Marshal(msg)
	_, err := conn.Write(append(data, '\n'))
	return err
}

func gerarValor() int64 {
	return int64(rand.Intn(8))
}

// -------- HEARTBEAT ----------

func heartbeat(conn net.Conn, id string, timeout time.Duration) {
	ticker := time.NewTicker(timeout / 2) // checa 2x mais rápido que o timeout
	defer ticker.Stop()

	for range ticker.C {

		// tentativa de escrita (ping)
		msg := MensagemServico{
			Tipo:      "servico",
			ID:        id,
			Timestamp: time.Now().UnixNano(),
		}

		data, _ := json.Marshal(msg)

		_, err := conn.Write(append(data, '\n'))
		if err != nil {
			return
		}	
	}
}

// -------- Main ----------

func main() {

	rand.Seed(time.Now().UnixNano())

	id := fmt.Sprintf("Servico-%d", rand.Intn(1000))

	msg := MensagemServico{
		Tipo: "servico",
		ID:   id,
		Timestamp: time.Now().UnixNano(),
	}

	brokerAddr := "localhost:8000"
	conn := conectarBroker(brokerAddr, msg)
	defer conn.Close()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// inicia heartbeat
	go heartbeat(conn, id, 5*time.Second)

	for range ticker.C {
		msg.Timestamp = time.Now().UnixNano()
		msg.Prioridade = gerarValor()

		err := enviarMensagem(conn, msg)

		if err != nil {
			fmt.Printf("(%s) [Servico %s] - [CONN]: falha ao enviar, reconectando...\n",
				timeStamp(), id)

			conn.Close()
			conn = conectarBroker(brokerAddr, msg)

			go heartbeat(conn, id, 5*time.Second)
			continue
		}

		fmt.Printf("(%s) [Servico %s] - [Servico]: Prioridade enviada=%d\n",
			timeStamp(), id, msg.Prioridade)
	}
}