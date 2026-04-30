package main

import (
	"fmt"
	"time"
	"math/rand"
)

func main() {
	// Criando a fila com taxa de envelhecimento de 0.5
	fila := &FilaPrioridade{}

	// 1. Inserindo dados
	fila.Push(Requisicao{Prioridade: 10.0, Timestamp: 1625000000, Origem: "User_A"})
	fila.Push(Requisicao{Prioridade: 5.0,  Timestamp: 1625000010, Origem: "User_B"})
	fila.Push(Requisicao{Prioridade: 10.0, Timestamp: 1625000005, Origem: "User_C"}) // Mesmo nível de A, mas timestamp maior

	fmt.Println("Fila Inicial:")
	fila.Listar()

	// 2. Removendo (O que dispara o Aging)
	atendido := fila.Remove()
	fmt.Printf("Atendido: %s (Prioridade original era 10.0)\n\n", atendido.Origem)

	fmt.Println("Fila após 1ª Remoção e efeito do Aging:")
	fila.Listar()

	for i := 0 ; i < 10; i++ {
		fila.Push(Requisicao{Prioridade: rand.Intn(7), Timestamp: int64(rand.Intn(1000)), Origem: fmt.Sprintf("User_%d", rand.Intn(100))})
		time.Sleep(time.Second)
	}


	fmt.Println("Requisição removida: ", fila.Remove())
	fila.Listar()

	fmt.Println("Requisição removida: ", fila.Remove())
	fila.Listar()

	fmt.Println("Requisição removida: ", fila.Remove())
	fila.Listar()
}
