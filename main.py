"""
Implementation of Simple Gossip Protocol.
author: Tedi Mitiku
date: 12/15/2021
"""
import argparse
from node import GossipNode

parser = argparse.ArgumentParser(description='Gossip Protocol')
parser.add_argument('--data', dest='filepath',
                    help='Filepath to initial database contents of node')
parser.add_argument('--port', dest='port', type=int,
                    help='Port this node will run on')
parser.add_argument('--peers', nargs='+', dest='peers', 
                    help='Port numbers that peers in network are running on')

if __name__ == '__main__':
  args = parser.parse_args()
  node = GossipNode(args.filepath, args.port, args.peers)
  print('----------------------------------------------')
  print(f'INFO: STARTING GOSSIP NODE at 127.0.0.1:{args.port}')
  print('----------------------------------------------')
  node.run()
