###############################################################################
#
# Filename: delete.py
# Author: Israel O. Dilan
#
# Description:
# 	Delete client for the DFS
#
#

import socket
import sys
from os import remove
from Packet import *

def usage():
	print "Usage:\n\tFrom DFS: python " + sys.argv[0] + " <server>:<port>:<dfs file path>\n\t"
	sys.exit(0)
	
def delFromDFS(address, fname):
	""" Contact the metadata server to ask for the file blocks of
		the file fname.  Delete the data blocks from the data nodes.
		Saves the data in path.
	"""

	# Contact the metadata server to ask for information of fname
	# Fill code
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	# If there is no error response Retreive the data blocks
	# Fill code
	sock.connect(address) 
	p = Packet()
	p.BuildDelPacket(fname)
	sock.sendall(p.getEncodedPacket())
		
	# Save the file
	# Fill code
	received = sock.recv(4096)
	p.DecodePacket(received)
	# p.GetDataBlocks(received)
	s = ""
	for i, j, k in p.getDataNodes():
		print i, j, k
		sockete = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sockete.connect((i, int(j)))
		p.BuildDelDataBlockPacket(k)
		sockete.sendall(p.getEncodedPacket())
		s += sockete.recv(4096)
		print k, s 
		sockete.close()

	print received
	# if received == "OK":
	# 	try: 
	# 		os.remove(fname)
	# 	except:
	# 		pass
	# 	else:
	# 		pass
	
	sock.close()

if __name__ == "__main__":
#	client("localhost", 8000)
	if len(sys.argv) != 2:
		usage()

# From DFS: python del.py <server>:<port>:<dfs file path>
	file = sys.argv[1].split(":")

	ip = file[0]
	port = int(file[1])
	file_path = file[2]

delFromDFS((ip, port), file_path)