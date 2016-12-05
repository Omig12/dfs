###############################################################################
#
# Filename: data-node.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	data node server for the DFS
#

from Packet import *

import sys
import socket
import SocketServer
import uuid
import os.path
from os import remove

def usage():
	print """Usage: python %s <server> <port> <data path> <metadata port,default=8000>""" % sys.argv[0] 
	sys.exit(0)


def register(meta_ip, meta_port, data_ip, data_port):
	"""Creates a connection with the metadata server and
	   register as data node
	"""

	# Establish connection
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	# Fill code	
	sock.connect((meta_ip, meta_port))
	# sock.sendall(data + "\n")

	try:
		response = "NAK"
		sp = Packet()
		while response == "NAK":
			sp.BuildRegPacket(data_ip, data_port)
			sock.sendall(sp.getEncodedPacket())
			response = sock.recv(4096)
			print response

			if response == "DUP":
				print "Duplicate Registration"

			if response == "NAK":
				print "Registratation ERROR"

	finally:
		sock.close()
	

class DataNodeTCPHandler(SocketServer.BaseRequestHandler):

	def handle_put(self, p):
		"""Receives a block of data from a copy client, and 
		   saves it with an unique ID.  The ID is sent back to the
		   copy client.
		"""
		global PORT
		fname, fsize = p.getFileInfo()

		# Generates an unique block id.
		blockid = str(uuid.uuid1())


		# Open the file for the new data block.  
		# Receive the data block.
		# Send the block id back

		# Fill code
		get_chunk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		get_chunk.bind(("", PORT +1))
		get_chunk.listen(3)
		self.request.send("OK")
		conn, addr = get_chunk.accept()
		data = conn.recv(4096)
		print data
		conn.sendall(blockid)
		conn.close()

		with open(blockid , 'w+') as file:
			file.write(data)
		get_chunk.close()


	def handle_get(self, p):
		
		# Get the block id from the packet
		blockid = p.getBlockID()


		# Read the file with the block id data
		# Send it back to the copy client.
		data = ""
		with open(blockid , 'r') as file:
			data = file.read()

		# Fill code
		self.request.send(data)

	def handle_del(self, p):
		
		# Get the block id from the packet
		blockid = p.getBlockID()

		# Delete the file with the block id data
		# Send it back to the copy client.
		# Fill code
		os.remove(blockid)
		self.request.send("%s Deleted".format(blockid))



	def handle(self):
		msg = self.request.recv(4096)
		print msg, type(msg)

		p = Packet()
		p.DecodePacket(msg)

		cmd = p.getCommand()
		if cmd == "put":
			self.handle_put(p)

		elif cmd == "get":
			self.handle_get(p)

		elif cmd == "del":
			self.handle_del(p)
		

if __name__ == "__main__":

	META_PORT = 8000
	if len(sys.argv) > 4:
		usage()

	try:
		HOST = sys.argv[1]
		PORT = int(sys.argv[2])
		DATA_PATH = sys.argv[3]

		if len(sys.argv) > 4:
			META_PORT = int(sys.argv[4])

		if not os.path.isdir(DATA_PATH):
			print "Error: Data path %s is not a directory." % DATA_PATH
			usage()
	except:
		usage()

	register("localhost", META_PORT, HOST, PORT)
	server = SocketServer.TCPServer((HOST, PORT), DataNodeTCPHandler)

	# Activate the server; this will keep running until you
	# interrupt the program with Ctrl-C
	server.serve_forever()