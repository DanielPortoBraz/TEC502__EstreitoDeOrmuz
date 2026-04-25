package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// -------- Struct --------

type MensagemDrone struct {
	Tipo  string `json:"tipo"`
	ID    string `json:"id"`
	Acao  string `json:"acao"` // requisicao | andamento | conclusao
	Sinal bool   `json:"sinal"`
}

// -------- Estados --------

const (
	FREE = "FREE"
	BUSY = "BUSY"
)

// -------- Drone --------

type Drone struct {
	mu     sync.Mutex
	estado string
	id     string

	brokers []string
}

// -------- Inicialização --------

func novoDrone(id string, brokers []string) *Drone {
	return &Drone{
		id:      id,
		estado:  FREE,
		brokers: brokers,
	}
}

// -------- Conexão --------

func (d *Drone) conectarBrokers() {
	for _, addr := range d.brokers {
		go d.conectarBroker(addr)
	}
}

func (d *Drone) conectarBroker(addr string) {
	for {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Erro ao conectar ao broker:", addr)
			time.Sleep(2 * time.Second)
			continue
		}

		fmt.Println("Conectado ao broker:", addr)
		d.enviarMensagem(conn, "conexao", false)

		d.handleConn(conn)

		fmt.Println("Desconectado do broker:", addr)
		time.Sleep(2 * time.Second)
	}
}

// -------- Comunicação --------

func (d *Drone) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		msgStr, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		var msg MensagemDrone
		err = json.Unmarshal([]byte(msgStr), &msg)
		if err != nil {
			continue
		}

		d.handleMensagem(conn, msg)
	}
}

func (d *Drone) enviarMensagem(conn net.Conn, acao string, sinal bool) {
	msg := MensagemDrone{
		Tipo:  "drone",
		ID:    d.id,
		Acao:  acao,
		Sinal: sinal,
	}

	data, _ := json.Marshal(msg)
	conn.Write(append(data, '\n'))
}

// -------- Lógica --------

func (d *Drone) handleMensagem(conn net.Conn, msg MensagemDrone) {

	if msg.Acao == "requisicao" {

		d.mu.Lock()

		if d.estado == BUSY {
			fmt.Println("Drone ocupado, ignorando requisição")
			d.mu.Unlock()
			return
		}

		d.estado = BUSY
		d.mu.Unlock()

		fmt.Println("Requisição aceita de broker")

		go d.executarTarefa(conn)
	}
}

func (d *Drone) executarTarefa(conn net.Conn) {

	// andamento
	d.enviarMensagem(conn, "andamento", false)

	// simulação de trabalho
	time.Sleep(5 * time.Second)

	// conclusão
	d.enviarMensagem(conn, "conclusao", true)

	fmt.Println("Tarefa concluída")

	d.mu.Lock()
	d.estado = FREE
	d.mu.Unlock()
}

// -------- Main --------

func main() {

	brokers := []string{
		"localhost:8080",
	}

	id := fmt.Sprintf("drone-%d", time.Now().UnixNano()%10000)

	drone := novoDrone(id, brokers)
	drone.conectarBrokers()

	select {}
}