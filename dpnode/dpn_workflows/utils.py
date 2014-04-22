import os
import sys
import ctypes
import random
import platform

from dpnode.settings import DPN_NODE_LIST

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