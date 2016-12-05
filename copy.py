###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	Copy client for the DFS
#
#

import socket
import sys
import os.path

from Packet import *

def usage():
	print """Usage:\n\tFrom DFS: python %s <server>:<port>:<dfs file path> <destination file>\n\tTo   DFS: python %s <source file> <server>:<port>:<dfs file path>""" % (sys.argv[0], sys.argv[0])
	sys.exit(0)

def copyToDFS(address, fname, path):
	""" Contact the metadata server to ask to copy file fname,
		get a list of data nodes. Open the file in path to read,
		divide in blocks and send to the data nodes. 
	"""

	# Create a connection to the data server
	# Fill code
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	
	# Connect to server and send data
	sock.connect(address) 
	p = Packet()
	fsize = os.path.getsize(path)
	p.BuildPutPacket(fname, fsize)
	sock.sendall(p.getEncodedPacket())

	received = sock.recv(4096)
	try:
		p.DecodePacket(received)
		nodes = 0
		for i in list(enumerate(p.getDataNodes(), start = 1)):
			nodes += 1
			print str(i).translate(None,'u[],\'()').replace(' ', "\t")

		# Read file
		# Fill code
		with open(path , 'r') as file:
			s = file.read()
		split = [s[i:i+(len(s)//nodes)] for i in range(0, len(s), len(s)//nodes)]
		if (len(split) > nodes):
			split[len(split)-2] += split[len(split) - 1]
			split.pop()
		print len(split), "\n" 
		for j in split:
			print j

		# Create a Put packet with the fname and the length of the data,
		# and sends it to the metadata server
		# Fill code
		blockid = []
		x = 0
		for n in p.getDataNodes():
			sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sender.connect((str(n[0]), int(n[1])))
			p.BuildPutPacket(fname, len(split[x]))
			sender.sendall(p.getEncodedPacket())
			print sender.recv(4096)
			sender.close()
			sp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sp.connect((str(n[0]), int(n[1])+1))
			sp.sendall(split[x])
			bid = sp.recv(4096)
			blockid.append((str(n[0]), int(n[1]), bid))
			sp.close()
			x += 1
		print blockid
		

		# If no error or file exists
		# Get the list of data nodes.
		# Divide the file in blocks
		# Send the blocks to the data servers
		# Fill code	
		sen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sen.connect(address)

		# Notify the metadata server where the blocks are saved.
		# Fill code
		p.BuildDataBlockPacket(fname, blockid)
		sen.sendall(p.getEncodedPacket())
		sen.close()

	except:
		print received
	
def copyFromDFS(address, fname, path):
	""" Contact the metadata server to ask for the file blocks of
		the file fname.  Get the data blocks from the data nodes.
		Saves the data in path.
	"""
	# From DFS: python %s <server>:<port>:<dfs file path> <destination file>

	# Contact the metadata server to ask for information of fname
	# Fill code
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	# If there is no error response Retreive the data blocks
	# Fill code
	sock.connect(address) 
	p = Packet()
	p.BuildGetPacket(fname)
	sock.sendall(p.getEncodedPacket())
		
	# Save the file
	# Fill code
	received = sock.recv(4096)
	p.DecodePacket(received)
	# p.GetDataBlocks(received)
	s = ""
	for i, j, k in p.getDataNodes():
		print i, j
		sockete = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sockete.connect((i, int(j)))
		p.BuildGetDataBlockPacket(k)
		sockete.sendall(p.getEncodedPacket())
		s += sockete.recv(4096)
		print k, s 
		sockete.close()

	with open(path , 'w+') as file:
		file.write(s)

	sock.close()

if __name__ == "__main__":
#	client("localhost", 8000)
	if len(sys.argv) < 3:
		usage()

	file_from = sys.argv[1].split(":")
	file_to = sys.argv[2].split(":")

	if len(file_from) > 1:
		ip = file_from[0]
		port = int(file_from[1])
		from_path = file_from[2]
		to_path = sys.argv[2]

		if os.path.isdir(to_path):
			print "Error: path %s is a directory.  Please name the file." % to_path
			usage()

		copyFromDFS((ip, port), from_path, to_path)

	elif len(file_to) > 2:
		ip = file_to[0]
		port = int(file_to[1])
		to_path = file_to[2]
		from_path = sys.argv[1]

		if os.path.isdir(from_path):
			print "Error: path %s is a directory.  Please name the file." % from_path
			usage()

		copyToDFS((ip, port), to_path, from_path)


