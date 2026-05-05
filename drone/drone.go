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
	Tipo      string `json:"tipo"`
	ID        string `json:"id"`
	Acao      string `json:"acao"` // conexao | requisicao | andamento | conclusao | heartbeat | estado
	Sinal     bool   `json:"sinal"`
	Estado    string `json:"estado"`
	Timestamp int64  `json:"timestamp"`
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
	fmt.Printf("(%s) [Drone %s] - [INIT]: iniciado (estado=FREE)\n", timeStamp(), id)

	return &Drone{
		id:      id,
		estado:  FREE,
		brokers: brokers,
	}
}

// ---------- Heartbeat -----------

func heartbeat(conn net.Conn, d *Drone, timeout time.Duration) {
	ticker := time.NewTicker(timeout / 2)
	defer ticker.Stop()

	for range ticker.C {
		msg := MensagemDrone{
			Tipo:      "drone",
			ID:        d.id,
			Acao:      "heartbeat",
			Estado: d.estado,
			Timestamp: time.Now().UnixNano(),
		}

		data, _ := json.Marshal(msg)
		_, err := conn.Write(append(data, '\n'))
		if err != nil {
			// Conexão caiu: encerra a goroutine silenciosamente.
			// handleConn detectará o erro na próxima leitura e iniciará a reconexão.
			return
		}
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

		// Heartbeat a cada 5s
		go heartbeat(conn, d, 5*time.Second)

		// Mensagem de Handshake
		// Envia estado atual na mensagem de conexão para que Broker saiba se poderá mandar requisição.
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
		if err = json.Unmarshal([]byte(msgStr), &msg); err != nil {
			continue
		}

		d.handleMensagem(conn, msg)
	}
}

func (d *Drone) enviarMensagem(conn net.Conn, acao string, sinal bool) {

	d.mu.Lock()
	estadoAtual := d.estado
	d.mu.Unlock()

	msg := MensagemDrone{
		Tipo:      "drone",
		ID:        d.id,
		Acao:      acao,
		Sinal:     sinal,
		Estado:    estadoAtual,
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
			fmt.Printf("(%s) [Drone %s] - [DRONE]: ocupado, ignorando requisição\n",
				timeStamp(), d.id)
			d.mu.Unlock()
			d.enviarMensagem(conn, "estado", false) // responde BUSY para o broker atualizar o map
			return
		}

		d.estado = BUSY
		d.mu.Unlock()

		// Informa BUSY antes de começar, para o broker atualizar seu map
		d.enviarMensagem(conn, "estado", false)

		fmt.Printf("(%s) [Drone %s] - [DRONE]: requisição aceita\n", timeStamp(), d.id)

		go d.executarTarefa(conn)
	}
}

func (d *Drone) executarTarefa(conn net.Conn) {

	fmt.Printf("(%s) [Drone %s] - [DRONE]: tarefa iniciada\n", timeStamp(), d.id)

	d.enviarMensagem(conn, "andamento", false)

	fmt.Printf("(%s) [Drone %s] - [DRONE]: em execução\n", timeStamp(), d.id)

	time.Sleep(5 * time.Second)

	fmt.Printf("(%s) [Drone %s] - [DRONE]: tarefa concluída\n", timeStamp(), d.id)

	d.mu.Lock()
	d.estado = FREE
	d.mu.Unlock()

	// Informa FREE para o broker liberar o slot e processar próxima req da fila
	d.enviarMensagem(conn, "conclusao", true)

	fmt.Printf("(%s) [Drone %s] - [DRONE]: estado=FREE\n", timeStamp(), d.id)
}

// -------- Utils --------

func timeStamp() string {
	t := time.Now()
	return fmt.Sprintf("%d-%d-%d %d:%d:%d",
		t.Day(), t.Month(), t.Year(),
		t.Hour(), t.Minute(), t.Second())
}

// -------- Main --------

func main() {

	brokers := []string{
		"localhost:8000",
		"localhost:8001",
		"localhost:8002",
		"localhost:8003",
	}

	id := fmt.Sprintf("drone-%d", time.Now().UnixNano()%10000)

	drone := novoDrone(id, brokers)
	drone.conectarBrokers()

	select {}
}