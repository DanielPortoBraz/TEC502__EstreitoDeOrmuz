# 🛰️ Sistema de Setores Marítimos — Distribuído com Drones

> **Projeto Avaliativo — TEC502: MI - Concorrência e Conectividade**
> Universidade Estadual de Feira de Santana (UEFS)

Sistema distribuído em Go para gerenciamento de setores marítimos, onde Brokers coordenam o despacho de Drones a partir de requisições enviadas por Recursos e Serviços, utilizando o algoritmo de Ricart-Agrawala adaptado para múltiplos recursos compartilhados.

---

## 📐 Arquitetura Geral

O sistema é composto por quatro tipos de entidades:

| Entidade | Papel |
|---|---|
| **Broker** | Central de cada setor. Recebe requisições, coordena com outros Brokers via RA e despacha Drones |
| **Recurso / Serviço** | Clientes que enviam requisições de prioridade ao Broker do seu setor |
| **Drone** | Executor de tarefas. Conecta-se aos Brokers e aguarda requisições para entrar em execução |
| **Tester** | Ferramenta de teste interativa que simula um Broker, permitindo envio de requisições, pings e participação no protocolo RA |

Os Brokers formam uma **rede totalmente conectada (ponto a ponto)** entre si. Cada Broker mantém sua própria **fila de prioridade** de requisições e negocia o uso dos Drones (região crítica) com os demais via mensagens `REQUEST` / `OK`.

---

## 🔄 Fluxo Básico

<img width="1500" height="1200" alt="PBL2 Go - Arquitetura Geral" src="https://github.com/user-attachments/assets/1e642dab-cb6d-4433-9127-0d2fff2ea07f" />

O diagrama acima representa o sistema em operação com 4 Brokers, cada um sendo o centro de um setor. Recursos e Serviços são clientes externos que enviam requisições ao Broker do seu setor. Os Drones são recursos compartilhados posicionados no centro, acessíveis por qualquer Broker. Os Brokers se comunicam entre si em rede totalmente conectada (ponto a ponto) via mensagens `REQUEST / OK` do protocolo Ricart-Agrawala.

O fluxo completo de uma requisição segue as etapas abaixo:

**1. Entrada da requisição**
Recurso ou Serviço envia periodicamente uma mensagem com campo `prioridade` ao seu Broker. O Broker insere a requisição na sua **fila de prioridade local** caso o valor ultrapasse o limiar configurado.

**2. Tentativa de despacho**
O Broker verifica a quantidade de Drones livres disponíveis:

- **Drones suficientes** (livres ≥ número de Brokers + 1): o Broker despacha o Drone diretamente, sem acionar o protocolo RA. Cada Broker consegue atender sua fila de forma independente.
- **Drones escassos**: o recurso é disputado — o Broker inicia o protocolo Ricart-Agrawala.

**3. Protocolo Ricart-Agrawala (disputa)**
O Broker interessado faz broadcast de `REQUEST` para todos os seus peers, carregando o relógio lógico de Lamport e os dados da requisição. Cada peer responde com `OK` imediatamente se não estiver disputando, ou **adia o OK** caso tenha uma requisição de maior prioridade em disputa. Ao receber `OK` de todos os peers, o Broker entra na seção crítica.

**4. Execução pelo Drone**
O Broker seleciona um Drone livre do seu mapa, envia a requisição e aguarda. O Drone executa a tarefa por ~5 segundos e devolve o sinal de conclusão ao Broker.

**5. Saída da seção crítica**
O Broker marca a seção crítica como concluída, envia os `OK`s que havia adiado para os peers que aguardavam, e tenta despachar o próximo item da fila.

---

## 📁 Estrutura do Projeto

```
TEC502_ESTREITODEORMUZ/
├── broker/
│   ├── broker.go       # Lógica central do Broker (servidor TCP, RA, fila, dispatcher)
│   ├── fila.go         # Fila de prioridade com Aging
│   ├── mensagens.go    # Definição dos tipos de mensagem
│   ├── cert.pem        # Certificado TLS
│   ├── key.pem         # Chave privada TLS
│   ├── Dockerfile
│   └── go.mod
├── drone/
│   ├── drone.go        # Cliente Drone (heartbeat, execução de tarefas)
│   ├── Dockerfile
│   └── go.mod
├── recurso/
│   ├── recurso.go      # Cliente Recurso (envia requisições periódicas com prioridade)
│   ├── Dockerfile
│   └── go.mod
├── servico/
│   ├── servico.go      # Cliente Serviço (semelhante ao Recurso)
│   ├── Dockerfile
│   └── go.mod
├── test/
│   ├── tester.go       # Ferramenta de teste interativa (simula Broker via terminal)
│   ├── fila.go
│   ├── mensagens.go
│   ├── cert.pem
│   ├── key.pem
│   ├── Dockerfile
│   └── go.mod
├── docker-compose.yml
└── README.md
```

> Os arquivos `cert.pem` e `key.pem` já estão incluídos nos diretórios `broker/` e `test/`. Caso precise regenerá-los, veja a seção de configuração abaixo.

---

## ⚙️ Configuração

O sistema é executado **exclusivamente via Docker Compose**. Cada máquina física roda um Broker (e seus clientes associados). Antes de subir os contêineres, dois arquivos precisam ser ajustados conforme o ambiente de rede.

### 1. Endereços dos Brokers — `broker/broker.go`

Edite a função `resolveAddress` para mapear cada porta ao IP real da máquina que irá rodar aquele Broker:

```go
func resolveAddress(id string) string {
    switch id {
    case "8000":
        return "192.168.X.A:8000"  // IP da máquina que roda broker1
    case "8001":
        return "192.168.X.B:8001"  // IP da máquina que roda broker2
    case "8002":
        return "192.168.X.C:8002"  // IP da máquina que roda broker3
    }
    return ""
}
```

### 2. Endereços no `docker-compose.yml`

Substitua os placeholders `x.x.x.N` pelos IPs reais em todos os serviços:

```yaml
# Variável de ambiente do Drone
environment:
  - BROKERS=192.168.X.A:8000,192.168.X.B:8001,192.168.X.C:8002

# Argumento do Recurso/Serviço (aponta para o broker local da máquina)
command: ["./recurso", "192.168.X.A:8000"]

# Argumento do Tester (aponta para todos os brokers)
command: ["./tester", "192.168.X.A:8000", "192.168.X.B:8001", "192.168.X.C:8002"]
```

### Regenerando os Certificados TLS (opcional)

Os certificados já estão incluídos no repositório. Caso precise regenerá-los:

```bash
openssl req -x509 -newkey rsa:2048 -keyout broker/key.pem -out broker/cert.pem \
  -days 365 -nodes -subj "/CN=localhost"

# Copie também para o diretório test/ se necessário
cp broker/cert.pem test/cert.pem
cp broker/key.pem  test/key.pem
```

---

## ▶️ Execução

O projeto é distribuído entre múltiplas máquinas físicas, cada uma responsável por um Broker. Todos os passos abaixo devem ser realizados **em cada máquina**, após configurar os IPs conforme descrito acima.

### Subindo os serviços

```bash
docker compose up --build
```

Cada máquina subirá os contêineres definidos no `docker-compose.yml`. A topologia padrão é:

| Serviço | Descrição |
|---|---|
| `broker1` | Broker na porta `8000`, conectado aos peers `8001` e `8002` |
| `drone1` | Drone que se conecta a todos os Brokers via variável `BROKERS` |
| `recurso_b1` | Recurso que envia requisições periódicas ao `broker1` |
| `servico_b1` | Serviço que envia requisições periódicas ao `broker1` |
| `tester` | Ferramenta interativa conectada a todos os Brokers |

> Em uma implantação com 3 máquinas, cada uma roda o `docker-compose.yml` ajustado para o seu Broker (`broker1`, `broker2` ou `broker3`), comentando ou removendo os blocos dos outros Brokers.

### Acessando o terminal do Tester

O Tester requer TTY interativo. Para acessá-lo após subir os contêineres:

```bash
docker attach tester
```

### Derrubando os serviços

```bash
docker compose down
```

---

## 🧪 Tester — Ferramenta de Teste Interativa

O `tester` se conecta aos Brokers como um peer e permite controlar o sistema via terminal. Após iniciar, um menu de comandos é exibido:

```
╔══════════════════════════════════════════╗
║           TESTER — Sistema Brokers       ║
╠══════════════════════════════════════════╣
║  ping    <addr>                          ║
║  req     <addr> <prioridade> <n>         ║
║  autoOK  <on|off>   (padrão: on)         ║
║  ok      <brokerID>                      ║
║  quit                                    ║
╚══════════════════════════════════════════╝
```

| Comando | Descrição |
|---|---|
| `ping <addr>` | Mede a latência RTT com o Broker no endereço informado (3 amostras, exibe min/avg/max/mdev) |
| `req <addr> <prioridade> <n>` | Envia `n` requisições com a prioridade dada ao Broker |
| `autoOK <on\|off>` | Liga/desliga resposta automática de OK ao receber REQUESTs do RA (padrão: `on`) |
| `ok <brokerID>` | Envia OK manualmente para um broker (usado quando `autoOK off`) |
| `quit` | Encerra o tester |

O tester exibe em tempo real todos os eventos do protocolo RA (`REQUEST` e `OK`) recebidos dos Brokers conectados.

---

## 🔑 Conceitos Técnicos Principais

### Ricart-Agrawala Adaptado
O algoritmo clássico de exclusão mútua distribuída foi adaptado para múltiplos recursos (Drones). Quando há Drones suficientes para todos os Brokers, o despacho é direto (sem disputa). Quando os Drones são escassos, o protocolo RA é acionado: o Broker interessado faz broadcast de `REQUEST` com seu relógio de Lamport e aguarda `OK` de todos os peers antes de entrar na seção crítica.

### Relógio Lógico de Lamport
Usado para ordenar e desempatar requisições concorrentes. O relógio local de cada Broker é incrementado a cada evento significativo e sincronizado ao receber mensagens dos peers.

### Fila de Prioridade com Aging
Requisições entram na fila ordenadas por prioridade (maior valor = maior urgência). A cada despacho, todos os itens restantes têm sua prioridade incrementada (aging), evitando inanição de requisições de baixa prioridade.

### Heartbeat
Todos os dispositivos enviam heartbeats periódicos. O Broker monitora os timestamps recebidos e remove conexões silenciosas, tratando desconexões de Brokers (reavaliação de quórum do RA) e de Drones (busca por substituto ou devolução da requisição à fila).

### TLS sobre TCP
Toda comunicação usa TLS com certificado autoassinado. Clientes (Drones, Recursos, Serviços, Tester) conectam com `InsecureSkipVerify: true`; Brokers carregam `cert.pem` + `key.pem` para servir conexões seguras.

---

## 📨 Tipos de Mensagem

Todas as mensagens trafegam como JSON delimitado por `\n`.

| Tipo | Campos relevantes | Descrição |
|---|---|---|
| `MensagemBroker` | `tipo`, `id`, `reply` (REQUEST/OK/HEARTBEAT/PING/PONG), `relogio`, `requisicao` | Comunicação entre Brokers (protocolo RA) |
| `MensagemDrone` | `tipo`, `id`, `acao` (conexao/requisicao/andamento/conclusao/heartbeat/estado/rejeitado), `sinal`, `estado` | Comunicação Broker ↔ Drone |
| `MensagemRecurso` | `tipo`, `id`, `prioridade`, `timestamp` | Requisição de Recurso ao Broker |
| `MensagemServico` | `tipo`, `id`, `prioridade`, `timestamp` | Requisição de Serviço ao Broker |

---

## 🧩 Casos de Escala Tratados

| Cenário | Comportamento |
|---|---|
| **Muitos Drones, poucos Brokers** | Bypass do RA — cada Broker despacha diretamente sem disputar |
| **Poucos Drones, muitos Brokers** | RA ativado — Brokers competem pelo recurso escasso |
| **1 Drone, N Brokers** | RA com quórum total — apenas um Broker por vez acessa o Drone |
| **Drone desconecta durante CS** | Broker busca substituto imediatamente; se não houver, aguarda novo Drone conectar |
| **Broker peer desconecta** | Quórum reavaliado; OKs adiados são liberados; reconexão automática após 3s |

---

## 📄 Licença

Este projeto foi desenvolvido para fins acadêmicos e avaliativos no âmbito da disciplina **TEC502 — MI: Concorrência e Conectividade** da Universidade Estadual de Feira de Santana (UEFS).

**Autor:** Daniel Porto Braz
