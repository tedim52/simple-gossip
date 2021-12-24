import logging
import random
import string
from dataclasses import dataclass


class IncorrectMessageFormat(Exception):
  pass

@dataclass
class GossipMessage:
  uuid: str
  node_id: str
  data: str

def create_messages(filepath, k, node_id):
  """ Parses .txt file at filepath and returns a collection of GossipMessage's.
    
    Args:
      filepath: A path to a file with data in it.
      k: the amount of cycles to propagate each GossipMessage.
      node_id: identifier of node that wants to create messages.

    Returns:
      messages: A dict of GossipMessage's in form { uuid : Message, k } 
  """
  messages = {}
  with open(filepath, 'r') as f:
    lines = f.readlines()
    for line in lines[1:]:
      uuid = (''.join(random.choice(string.ascii_letters) for x in range(4))).upper()
      node_id: node_id
      data = line
      message = GossipMessage(uuid, node_id, data)
      messages[uuid] = message, k
  return messages

def decode_message(msg):
  """ Takes in a string in the format { uuid|hostname|data } and returns a
    GossipMessage object. 
  
    Raises IncorrectMessageFormat for strings with wrong format. 
  """
  msg = msg.split("|")
  if len(msg) != 3 or len(msg[0]) < 4:
    raise IncorrectMessageFormat
  else:
    return GossipMessage(msg[0], msg[1], msg[2])

