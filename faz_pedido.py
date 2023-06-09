# docker compose up -d
from kafka import KafkaProducer

# Configurações do Kafka
bootstrap_servers = 'localhost:9092'
pedido_topic = 'pedidos-garcom'

# Cria o produtor Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Função para enviar pedidos
def enviar_pedido(pedido):
    # Gera a mensagem para o garçom
    mensagem = pedido.encode('utf-8')

    # Envia a mensagem para o Kafka
    producer.send(pedido_topic, value=mensagem)
    producer.flush()  # Garante o envio imediato da mensagem

# Exemplo de envio de pedidos
while True:
    pedido = input("Digite o pedido: ")

    # Chama a função para enviar o pedido ao garçom
    enviar_pedido(pedido)
