#!/usr/bin/env python3
# -*- coding: utf-8; mode: python -*-

"""XML utilities."""

import xml.dom.minidom as minidom
import xml.etree.ElementTree as etree


class Error(Exception):
    """Errors used in this module."""
    pass



Element = minidom.Element


def parse_from_string(text):
    """Parses an XML text into a DOM tree.

    Args:
        text: XML text to parse.
    Returns:
        The parsed XML document, as a tree of DOM elements.
    """
    return minidom.parseString(text)


def list_nodes_with_type(nodes, type):
    return filter(lambda n: n.nodeType == type, nodes)


def list_nodes_with_tag(nodes, tag):
    def _Filter(node):
        return ((node.nodeType == Element.ELEMENT_NODE) and (node.tagName == tag))
    return filter(_Filter, nodes)


def get_child_with_tag(node, tag):
    tags = list_nodes_with_tag(nodes=node.childNodes, tag=tag)
    filtered = list(tags)
    if len(filtered) == 0:
        return None
    elif len(filtered) == 1:
        return filtered[0]
    else:
        raise Error('Cannot find single child with tag %r in %s' % (tag, node.toxml()))


def get_child(node, *path):
    """Looks up an arbitrarily nested child node.

    Args:
        node: Root XML node to inspect.
        *path: Path (sequence of node names) to the child node to look up.
    Returns:
        The specified child node.
        None if the node path does not exist.
    """
    for path_comp in path:
        if node is not None:
            node = get_child_with_tag(node=node, tag=path_comp)
    return node


def get_node_text_value(node):
    """Reports the text value of a given XML node.

    For example, if node represents <a>Text</a>,
    this method returns 'Text'.

    If the node is empty (eg. <a/>), this returns ''.

    Args:
        node: XML text node to report the text value of.
    Returns:
        The text value of the node.
    """
    text_nodes = []
    for child in node.childNodes:
        # Filter out comments:
        if child.nodeType == Element.COMMENT_NODE:
            continue
        elif child.nodeType == Element.TEXT_NODE:
            text_nodes.append(child)
        else:
            raise Error('Invalid XML nodes for text value: %r', node.toxml())

    text = ''.join(map(lambda n: n.data, text_nodes))
    return text.strip()


def get_child_node_text_value(node, child_name):
    """Reports the text value of the specified child node.

    Args:
        node: XML node to inspect.
        child_name: Name of the immediate child text node to inspect.
    Returns:
        The text value of the specified child text node.
    """
    child_node = get_child(node, child_name)
    if child_node is None:
        return None
    return get_node_text_value(child_node)


def set_node_text_value(node, text):
    if len(node.childNodes) == 0:
        text_node = minidom.Text()
        node.appendChild(text_node)
    elif len(node.childNodes) == 1:
        text_node = node.firstChild
    else:
        raise Error('Invalid node with text value: %r' % node.toxml())
    assert (text_node.nodeType == Element.TEXT_NODE), node.toxml()
    text_node.data = text

