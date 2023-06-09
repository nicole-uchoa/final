import time
from kafka import KafkaConsumer, KafkaProducer
import queue
import threading
import unidecode
import unicodedata

# Configurações dos departamentos
departamentos = {
    'sanduiches': ['pao com carne', 'hamburguer', 'queijo'],
    'pratos_prontos': ['arroz', 'feijao', 'bife'],
    'bebidas': ['refrigerante', 'suco', 'agua'],
    'sobremesas': ['bolo', 'sorvete', 'frutas']
}

# Configurações do Kafka
bootstrap_servers = 'localhost:9092'
pedido_topic = 'pedidos-garcom'
pronto_topic = 'pedidos-prontos'

# Cria o consumidor e o produtor Kafka
try:
    consumer = KafkaConsumer(pedido_topic, bootstrap_servers=bootstrap_servers)
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
except Exception as e:
    print(e)
# Fila para armazenar os pedidos
fila_pedidos_bebidas = queue.Queue()
fila_pedidos_pratos_prontos = queue.Queue()
fila_pedidos_sanduiches = queue.Queue()
fila_pedidos_sobremesas = queue.Queue()

# clientes
cliente_bebida = queue.Queue()
cliente_sobremesa = queue.Queue()
cliente_pratos_prontos = queue.Queue()
cliente_sanduiches = queue.Queue()
# Função para processar os pedidos e enviá-los como prontos
def processar_pedidos_bebidas():
    try:
        while True:
            if fila_pedidos_bebidas.qsize() > 0:
                pedido = fila_pedidos_bebidas.get()  # Pega o primeiro pedido da fila
                cliente = cliente_bebida.get()
                print(f"Cliente {cliente} - Processando pedido: {pedido}")
                # Simula um tempo de processamento
                time.sleep(5)
                # Envia o pedido como pronto
                producer.send(pronto_topic, value=pedido.encode('utf-8'))
                print(f"Cliente {cliente} - Pedido pronto: {pedido}")
                producer.flush()  # Garante o envio imediato da mensagem
            else:
                time.sleep(1)  # Aguarda 1 segundo se a fila estiver vazia
    except Exception as e:
        print(f"Erro ao processar pedidos de bebidas: {str(e)}")

def processar_pedidos_pratos_prontos():
    while True:
        if fila_pedidos_pratos_prontos:
            pedido = fila_pedidos_pratos_prontos.get()  # Pega o primeiro pedido da fila
            cliente = cliente_pratos_prontos.get()
            print(f"Cliente {cliente} - Processando pedido: {pedido}")
            # Simula um tempo de processamento
            time.sleep(5)
            # Envia o pedido como pronto
            producer.send(pronto_topic, value=pedido.encode('utf-8'))
            print(f"Cliente {cliente} - Pedido pronto: {pedido}")
            producer.flush()  # Garante o envio imediato da mensagem
        else:
            time.sleep(1) 

def processar_pedidos_sanduiches():
    while True:
        if fila_pedidos_sanduiches:
            pedido = fila_pedidos_sanduiches.get()  # Pega o primeiro pedido da fila
            cliente = cliente_sanduiches.get()
            print(f"Cliente {cliente} - Processando pedido: {pedido}")
            # Simula um tempo de processamento
            time.sleep(5)
            # Envia o pedido como pronto
            producer.send(pronto_topic, value=pedido.encode('utf-8'))
            print(f"Cliente {cliente} - Pedido pronto: {pedido}")
            producer.flush()  # Garante o envio imediato da mensagem
        else:
            time.sleep(1) 

def processar_pedidos_sobremesas():
    while True:
        if fila_pedidos_sobremesas.qsize()>0:
            pedido = fila_pedidos_sobremesas.get()  # Pega o primeiro pedido da fila
            cliente = cliente_sobremesa.get()
            print(f"Cliente {cliente} - Processando pedido: {pedido}")
            # Simula um tempo de processamento
            time.sleep(5)
            # Envia o pedido como pronto
            producer.send(pronto_topic, value=pedido.encode('utf-8'))
            print(f"Cliente {cliente} - Pedido pronto: {pedido}")
            producer.flush()  # Garante o envio imediato da mensagem
        else:
            time.sleep(1) 

# Função para consumir os pedidos do garçom e adicioná-los à fila
def consumir_pedidos():
    for mensagem in consumer:
        pedido = mensagem.value.decode('utf-8')
        # número para identificar o cliente
        cliente = mensagem.offset
        # trata string de entrada
        pedido = str(pedido).lower()
        pedido = unidecode.unidecode(pedido)

        departamento = None
        for depto, alimentos in departamentos.items():
            if pedido in alimentos:
                departamento = depto
                break

        if departamento:
            if departamento == 'bebidas':
                cliente_bebida.put(cliente)
                fila_pedidos_bebidas.put(pedido)
                print(f"Cliente {cliente} - Pedido de bebidas recebido: {pedido}")
            elif departamento == 'pratos_prontos':
                cliente_pratos_prontos.put(cliente)
                fila_pedidos_pratos_prontos.put(pedido)
                print(f"Cliente {cliente} - Pedido de pratos prontos recebido: {pedido}")
            elif departamento == 'sanduiches':
                cliente_sanduiches.put(cliente)
                fila_pedidos_sanduiches.put(pedido)
                print(f"Cliente {cliente} - Pedido de sanduiches recebido: {pedido}")
            elif departamento == 'sobremesas':
                cliente_sobremesa.put(cliente)
                fila_pedidos_sobremesas.put(pedido)
                print(f"Cliente {cliente} - Pedido de sobremesas recebido: {pedido}")
        else:
            print("Pedido inválido!")

# Inicia as threads para consumir os pedidos e processá-los
consumer_thread = threading.Thread(target=consumir_pedidos)
processamento_thread = threading.Thread(target=processar_pedidos_bebidas)
processamento_thread2 = threading.Thread(target=processar_pedidos_pratos_prontos)
processamento_thread3 = threading.Thread(target=processar_pedidos_sobremesas)
processamento_thread4 = threading.Thread(target=processar_pedidos_sanduiches)

consumer_thread.start()
processamento_thread.start()
processamento_thread2.start()
processamento_thread3.start()
processamento_thread4.start()
