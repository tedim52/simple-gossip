import socket
import random
from threading import Thread, Lock
from utils import *

MESSAGE_COUNT=300

class GossipNode:
  """ A node that connects to peers on a P2P networks and propagates and receives 
    updates based on a simple gossip protocol.
  """
  def __init__(self, data, port, peers):
    """ Constructor for GossipNode.
    
      Attributes:
        data: a filepath containing data of "updates" that this node wants to propagate.
        ip_address: the ip address that this node is running on.
        port: the port that this node is running on.
        node_id: an identifier for this node in the format ip_address:port.
        peer_list: a string list of peers where peers are in the format node_ids. 
        database: a list of "updates" that this node has merged into its database.
        messages: a pool of messages this node is attempting to propate to peers on the network.
    """
    self.data = data
    self.ip_address = '127.0.0.1' # should be configurable
    self.port = port
    self.node_id = f'{self.ip_address}:{self.port}'
    self.peer_list = peers   
    self.__database = []
    self.__messages = create_messages(data, MESSAGE_COUNT, self.node_id)
    self.__receiver_connections = [] 
    self.__sender_connections = {}
    self.__msg_lock = Lock()

  def run(self):
    # print current database
    self.__output()

    # bootstrap the network
    client = Thread(target=self.__establish_connections)
    server = Thread(target=self.__receive_connections)
    client.start()
    server.start()
    
    client.join()
    server.join()

    # begin sending and receiving messages
    threads = []
    for client_socket in self.__receiver_connections:
      rec = Thread(target=self.__receive_messages, args=(client_socket,))
      rec.start()
      threads.append(rec)
    sender = Thread(target=self.__send_messages)
    sender.start()
    threads.append(sender)
    
    threads = [t.join() for t in threads]

    # output merged data
    self.__output()

  def __output(self):
    """ Prints and outputs all data this node has merged from the network to a 
      a .txt file.
    """
    self.__database.sort()
    with open(f'data/{self.node_id}.txt', 'w') as f:
      for data in self.__database:
        print(data)
        f.write(data)
        f.write('\n')
      f.close()

  def __establish_connections(self):
    """ Attempts to establish connections to all peers in peer_list. Terminates 
      once a connection has been made to every peer.
    """
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

  def __send_messages(self): 
    while not self.__terminate():
      self.__msg_lock.acquire()
      for uuid, (msg, cnt) in self.__messages.items():
        if cnt == 0:
          self.__database.append(msg.data)
          self.__messages[uuid] = msg, -1
        elif cnt > 0:
          cnt -= 1
          self.__messages[uuid] = msg, cnt
          message = uuid + '|' + msg.node_id + '|' + msg.data + '\n'
          receiving_peers = random.sample(self.peer_list, random.randint(1, len(self.peer_list) -1))
          for peer in receiving_peers:
            connection = self.__sender_connections[peer]
            try:
              connection.sendall(message.encode('utf-8'))
              print('%s | Sent message %s to %s' % (self.port, msg.uuid, peer))
            except:
              print('%s | Could not send message %s to %s' % (self.port, msg.uuid, peer))
      self.__msg_lock.release()

  def __receive_messages(self, client_socket):
    stream = ""
    while not self.__terminate():
      data = client_socket.recv(32)
      if '\n' in data.decode():
        eom = data.decode().split('\n')
        try:
          msg = decode_message(stream + eom[0])
          print('%s | Received the following message:' % (self.port))
          print(stream + eom[0])
          self.__msg_lock.acquire()
          if msg.uuid not in self.__messages.keys():
            self.__messages[msg.uuid] = msg, MESSAGE_COUNT
          self.__msg_lock.release()
        except IncorrectMessageFormat:
          pass
        stream = eom[2].lstrip()  if len(eom) > 2 else ""
      else:
        stream += data.decode()
    
  def __terminate(self):
    terminate = True
    self.__msg_lock.acquire()
    for _, (_, cnt) in self.__messages.items():
      if cnt != -1:
        terminate = False
    self.__msg_lock.release()
    return terminate


