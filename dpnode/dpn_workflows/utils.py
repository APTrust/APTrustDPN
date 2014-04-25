import os
import sys
import ctypes
import random
import platform

from dpnode.settings import DPN_NODE_LIST
from dpn_workflows.models import SequenceInfo

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
        