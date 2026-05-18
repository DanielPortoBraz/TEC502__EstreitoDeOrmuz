package main

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Requisicao representa o item da fila
type Requisicao struct {
	Prioridade int64 `json:"prioridade"`
	Timestamp  int64   `json:"timestamp"`
	Origem     string  `json:"origem"`
}

// FilaPrioridade gerencia as requisições
type FilaPrioridade struct {
	Itens      []Requisicao
}

// Push insere um novo item respeitando Prioridade > Timestamp
func (f *FilaPrioridade) Push(r Requisicao) {
	f.Itens = append(f.Itens, r)
	
	// Ordenação customizada:
	// 1. Prioridade (Maior valor) 
	sort.Slice(f.Itens, func(i, j int) bool {
		if f.Itens[i].Prioridade != f.Itens[j].Prioridade {
			return f.Itens[i].Prioridade > f.Itens[j].Prioridade
		}
		// Desempate por Timestamp (mais antigo ganha)
		return f.Itens[i].Timestamp < f.Itens[j].Timestamp
	})
}

// Remove retira o primeiro elemento e aplica o Aging nos restantes
func (f *FilaPrioridade) Remove() *Requisicao {
	if len(f.Itens) == 0 {
		return nil
	}

	// Extrai o primeiro
	item := f.Itens[0]
	f.Itens = f.Itens[1:]

	// Aplica o Aging após a remoção
	f.Aging()

	return &item
}

// Aging incrementa a prioridade de todos os itens restantes
func (f *FilaPrioridade) Aging() {
	for i := range f.Itens {
		f.Itens[i].Prioridade++
	}
	// Reordena após mudar as prioridades para manter a integridade
	sort.Slice(f.Itens, func(i, j int) bool {
		if f.Itens[i].Prioridade != f.Itens[j].Prioridade {
			return f.Itens[i].Prioridade > f.Itens[j].Prioridade
		}
		return f.Itens[i].Timestamp < f.Itens[j].Timestamp
	})
}

func (f *FilaPrioridade) Listar() {
	for _, item := range f.Itens {
		b, _ := json.Marshal(item)
		fmt.Println(string(b))
	}
	fmt.Println("--------------------")
}
