import os
import ctypes
import random
import platform
import requests
import hashlib
import logging

from uuid import uuid4

from dpnode.settings import DPN_REPLICATION_ROOT, DPN_FIXITY_CHOICES
from dpnode.settings import DPN_BAGS_FILE_EXT

from dpn_workflows.models import PROTOCOL_DB_VALUES
from dpn_workflows.models import SequenceInfo

logger = logging.getLogger('dpnmq.console')

def available_storage(path):
    """
    Returns path/drive available storage in bytes
    :param path: Directory to check free space.
    """

    # trying to be multi-plataform
    if platform.system() == 'Windows':
        available_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(
            ctypes.c_wchar_p(path), 
            None, 
            None, 
            ctypes.pointer(available_bytes)
        )

        free_bytes = available_bytes.value
    else:
        # using statvfs for Unix-based OS
        storage = os.statvfs(path)
        free_bytes = storage.f_bavail * storage.f_frsize
    
    return free_bytes

def choose_nodes(node_list):
    """
    Chooses the nodes to replicate with 
    based on some kind of match score 

    :param node_list: A list of acknowledge or available nodes 
    :returns: two appropiate nodes to replicate with.
    """
    
    # TODO: define a way or ranking to choose nodes
    # Doing random for now

    return random.sample(node_list, 1) 
    # TODO: change number to 2, now is 1 for testing purposes
    
def store_sequence(id,node_name,sequence_num):
    try:
        sequence = SequenceInfo.objects.get(correlation_id=id)
        sequence.sequence = "%s,%s" % (sequence.sequence,sequence_num)
        return sequence
    except SequenceInfo.DoesNotExist:
        sequence = SequenceInfo(correlation_id=id,node=node_name,sequence=str(sequence_num))
        sequence.save()
        return sequence
    
def validate_sequence(sequence_info):
    sequence = sequence_info.sequence.split(',')
    prev_num = -1
    
    for num in sequence:
        if int(num) <= prev_num:
            raise "Worklow sequence is out of sync in transaction %s from %s!" % (sequence_info.correlation_id, sequence_info.node)
        prev_num = int(num)
    
    return True

def download_bag(node, location, protocol):
    """
    Transfers the bag according to the selected protocol
    
    :param node: String of node name
    :param location: String url of the bag 
    :param protocol: selected protocol by node
    :returns: file
    """

    if protocol == 'https':        
        basefile = '%(node)s-%(local_id)s.%(ext)s' % {
            'node': node.upper(), 
            'local_id': str(uuid4()),
            'ext': DPN_BAGS_FILE_EXT
        }

        local_bagfile = os.path.join(DPN_REPLICATION_ROOT, basefile)

        r = requests.get(location, stream=True)
        with open(local_bagfile, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk:
                    f.write(chunk)
                    f.flush()

        return local_bagfile

    # elif protocol == 'rsync':
    # TODO: implement rsync transfer
    else:
        raise NotImplementedError

# NOTE: maybe change name to calculate_fixity_value or something
def fixity_str(bag_path, algorithm='sha256'):
    """
    Returns the fixity value for a given bag file
    stored in local 

    :param bag_path: The path of the local bag file
    :return 
    """
    blocksize = 65536

    if algorithm not in DPN_FIXITY_CHOICES:
        raise NotImplementedError

    if algorithm == 'sha256':        
        hasher = hashlib.sha256()
        with open(bag_path, 'rb') as f:
            buf = f.read(blocksize)
            while len(buf) > 0:
                hasher.update(buf)
                buf = f.read(blocksize)                
        return hasher.hexdigest()

    # TODO: implement hashing checksum for other algorithms

def protocol_str2db(protocol_str):
    """
    Returns the right value to be stored in database.
    We need to switch 'https' to 'H' or 'rsync' to 'R'

    :param protocol_str: String of the protocol like 'https' or 'rsync'
    """
    try:
        return PROTOCOL_DB_VALUES[protocol_str]
    except KeyError as err:
        raise KeyError('Mapping protocol key not found %s' % err)

def remove_bag(bag_path):
    """
    Removes a bag from the local replication directory of the current node

    :param bag_path: The path of the local bag file
    :return: Boolean
    """

    try:
        os.remove(bag_path)
    except OSError as err:
        logger.info(err)
        return False

    return True