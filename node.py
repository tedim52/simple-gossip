import socket
import random
from threading import Thread, Lock
from utils import GossipMessage, IncorrectMessageFormat, create_messages, decode_message, IncorrectMessageFormat

class GossipNode:
  """ A GossipNode that implements the simple Gossip protocol by connecting to 
    peers on a P2P network and propagating/receiving messages to peers.
    
    Attributes:
      data
      port
      ip_addres
      node_id
      peer_list
      peer_connections
      database
      messages
    """
  def __init__(self, data, port, peers):
    self.data = data
    self.port = port
    self.ip_address = '127.0.0.1'
    self.node_id = f'{self.ip_address}:{self.port}'
    self.peer_list = peers   
    self.__database = []
    self.__receiver_connections = [] 
    self.__sender_connections = {}
    self.__messages = create_messages(data, 5, self.node_id)
    self.connected = False
    self.connections = 0
    self.__msg_lock = Lock()

  def run(self):
    # ---- TESTING THAT CONNECTIONS ARE MADE ---- #
    # bootstrap the network
    client = Thread(target=self.__establish_connections) # attempts to connect to peers
    server = Thread(target=self.__receive_connections) # listens for connections from peers
    client.start()
    server.start()
    
    client.join()
    server.join()
    # for peer, connection in self.__sender_connections.items():
    #   print(peer)
    #   print(connection)
    # for client_socket in self.__receiver_connections:
    #   print(client_socket)
    # ---- END TESTING CONNECTIONS--------------- #

    threads = []
    for client_socket in self.__receiver_connections:
      rec = Thread(target=self.__receive, args=(client_socket,))
      rec.start()
      threads.append(rec)
    sender = Thread(target=self.__send)
    sender.start()
    threads.append(sender)
    
    threads = [t.join() for t in threads]

  def __output(self):
    pass
      
  def __establish_connections(self):
    connected = False
    while not connected:
      connections = 0
      for peer in self.peer_list:
        if peer not in self.__sender_connections.keys():
          try:
            connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connection.connect(('127.0.0.1', int(peer)))
            self.__sender_connections[peer] = connection
            connections += 1
            print('%s | Connected to %s' % (self.port, peer))
          except:
            print('%s | Cannot establish connections to %s' % (self.port, peer))
        else:
          connections += 1
      if connections == len(self.peer_list):
        connected = True

  def __receive_connections(self):
    try:
      print('%s | Listening for connections' % self.port)
      server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      server_socket.bind(('127.0.0.1', int(self.port)))
      server_socket.settimeout(10)
      server_socket.listen(5)
      while True:
          client_socket, address = server_socket.accept()
          self.__receiver_connections.append(client_socket)
          print('%s received a connection from %s' % (self.port, address[1]))
      server_socket.close()
    except OSError as os_error:
      print(os_error)

  def __send(self): 
    print('%s | Start sending messages to peers.' % (self.port))
    while True: # need to create termination condition(empty messages isn't stable)
      if self.__messages:
        self.__msg_lock.acquire()
        for uuid, (msg, cnt) in self.__messages.items():
          if cnt == 0:
            self.__database.append(msg.data)
            self.__messages[uuid] = msg, -1
          elif cnt > 0:
            cnt -= 1
            self.__messages[uuid] = msg, cnt
            receiving_peers = self.peer_list # eventually change to random
            print(receiving_peers)
            for peer in receiving_peers:
              # get the connection with that peer
              connection = self.__sender_connections[peer]
              # create message, encode, and send
              try:
                message = uuid + '|' + msg.node_id + '|' + msg.data + '\n'
                connection.sendall(message.encode('utf-8'))
                print('%s | Sent message %s to %s' % (self.port, msg.uuid, peer))
              except:
                print('%s | Could not send message %s to %s' % (self.port, msg.uuid, peer))
        self.__msg_lock.release() 

  def __receive(self, client_socket):
    print('%s | Starting to LISTEN for messages from %s.' % (self.port, client_socket.gethostname()))
    stream = ""
    while True:
      data = client_socket.recv(32)
      if '\n' in data.decode():
        eom = data.decode().split('\n')
        try:
          msg = decode_message(stream + eom[0])
          print('%s | Received the following message from %s:' % (self.port, client_socket.gethostname()))
          print(stream + eom[0])
          self.__msg_lock.acquire()
          if msg.uuid not in self.__messages.keys():
            self.__messages[msg.uuid] = msg, 5
          self.__msg_lock.release()
        except IncorrectMessageFormat:
          pass
        stream = eom[2].lstrip()  if len(eom) > 2 else ""
      else:
        stream += data.decode()

