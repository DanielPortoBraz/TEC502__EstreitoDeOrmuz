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
	Timestamp int64 `json:"timestamp"`
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

// ---------- Heartbeat -----------

func heartbeat(conn net.Conn, id string, timeout time.Duration) {
	ticker := time.NewTicker(timeout / 2) // checa 2x mais rápido que o timeout
	defer ticker.Stop()

	for range ticker.C {

		// tentativa de escrita (ping)
		msg := MensagemDrone{
			Tipo:      "drone",
			ID:        id,
			Acao: "heartbeat",
			Timestamp: time.Now().UnixNano(),
		}

		data, _ := json.Marshal(msg)

		_, err := conn.Write(append(data, '\n'))
		if err != nil {
			return
		}	
	}
}

// -------- Inicialização --------

func novoDrone(id string, brokers []string) *Drone {
	fmt.Printf("(%s) [Drone %s] - [INIT]: iniciado (estado=FREE)\n", timeStamp(), id)

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
			fmt.Printf("(%s) [Drone %s] - [CONN]: erro ao conectar %s\n", timeStamp(), d.id, addr)
			time.Sleep(2 * time.Second)
			continue
		}

		fmt.Printf("(%s) [Drone %s] - [CONN]: conectado ao broker %s\n", timeStamp(), d.id, addr)

		go heartbeat(conn, d.id, 5*time.Second)

		d.enviarMensagem(conn, "conexao", false)

		d.handleConn(conn)

		fmt.Printf("(%s) [Drone %s] - [CONN]: desconectado do broker %s\n", timeStamp(), d.id, addr)
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
			fmt.Printf("(%s) [Drone %s] - [CONN]: conexão encerrada\n", timeStamp(), d.id)
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
		Timestamp: time.Now().UnixNano(),
	}

	data, _ := json.Marshal(msg)
	conn.Write(append(data, '\n'))
}

// -------- Tratamento de Mensagens do Broker --------

func (d *Drone) handleMensagem(conn net.Conn, msg MensagemDrone) {

	if msg.Acao == "requisicao" {

		d.mu.Lock()

		if d.estado == BUSY {
			fmt.Printf("(%s) [Drone %s] - [DRONE]: ocupado, ignorando requisição\n", timeStamp(), d.id)
			d.mu.Unlock()
			return
		}

		d.estado = BUSY
		d.mu.Unlock()

		fmt.Printf("(%s) [Drone %s] - [DRONE]: requisição aceita\n", timeStamp(), d.id)

		go d.executarTarefa(conn)
	}
}

func (d *Drone) executarTarefa(conn net.Conn) {

	// INÍCIO
	fmt.Printf("(%s) [Drone %s] - [DRONE]: tarefa iniciada\n", timeStamp(), d.id)

	// andamento
	d.enviarMensagem(conn, "andamento", false)
	fmt.Printf("(%s) [Drone %s] - [DRONE]: em execução\n", timeStamp(), d.id)

	// simulação de trabalho
	time.Sleep(5 * time.Second)

	// conclusão
	d.enviarMensagem(conn, "conclusao", true)
	fmt.Printf("(%s) [Drone %s] - [DRONE]: tarefa concluída\n", timeStamp(), d.id)

	d.mu.Lock()
	d.estado = FREE
	d.mu.Unlock()

	fmt.Printf("(%s) [Drone %s] - [DRONE]: estado=FREE\n", timeStamp(), d.id)
}

// -------- Utils --------

// Retorna timeStamp atual
func timeStamp() string{
	currentTime := time.Now()

	return (fmt.Sprintf("%d-%d-%d %d:%d:%d",
		currentTime.Day(),
		currentTime.Month(),
		currentTime.Year(),
		currentTime.Hour(),
		currentTime.Minute(),
		currentTime.Second()))
}

// -------- Main --------

func main() {

	brokers := []string{
		"localhost:8000",
		"localhost:8001",
	}

	id := fmt.Sprintf("drone-%d", time.Now().UnixNano()%10000)

	drone := novoDrone(id, brokers)
	drone.conectarBrokers()

	select {}
}