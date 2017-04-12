from hail.java import Env, handle_py4j, scala_package_object

class TextTableConfig(object):
    """Configuration for delimited (text table) files.

    :param bool noheader: File has no header and columns the N columns are named ``_1``, ``_2``, ... ``_N`` (0-indexed)
    :param bool impute: Impute column types from the file
    :param comment: Skip lines beginning with the given pattern
    :type comment: str or None
    :param str delimiter: Field delimiter regex
    :param str missing: Specify identifier to be treated as missing
    :param types: Define types of fields in annotations files   
    :type types: str or None

    :ivar bool noheader: File has no header and columns the N columns are named ``_1``, ``_2``, ... ``_N`` (0-indexed)
    :ivar bool impute: Impute column types from the file
    :ivar comment: Skip lines beginning with the given pattern
    :vartype comment: str or None
    :ivar str delimiter: Field delimiter regex
    :ivar str missing: Specify identifier to be treated as missing
    :ivar types: Define types of fields in annotations files
    :vartype types: str or None
    """

    def __init__(self, noheader=False, impute=False,
                 comment=None, delimiter="\\t", missing="NA", types=None):
        self.noheader = noheader
        self.impute = impute
        self.comment = comment
        self.delimiter = delimiter
        self.missing = missing
        self.types = types

    def __str__(self):
        return self._to_java().toString()

    @handle_py4j
    def _to_java(self):
        """Convert to Java TextTableConfiguration object."""
        return Env.hail().utils.TextTableConfiguration.apply(self.types, self.comment,
                                                           self.delimiter, self.missing,
                                                           self.noheader, self.impute)

class FunctionDocumentation(object):

    @handle_py4j
    def types_rst(self, file_name):
        Env.hail().utils.FunctionDocumentation.makeTypesDocs(file_name)

    @handle_py4j
    def functions_rst(self, file_name):
        Env.hail().utils.FunctionDocumentation.makeFunctionsDocs(file_name)


def escape_identifier(str, escape_char='_'):
    """Escape string.  The result is an identifier: an identifier contains
    only letters, numbers and ``escape_char``, and starts with a
    letter or ``escape_char``.

    :param str str: String to escape.
    :param str escape_char: Escape character.

    :return: String escaped as an identifier.
    :rtype: str

    """
    
    return Env.hail().utils.StringEscapeUtils.escapeIdentifier(str, escape_char)

def unescape_identifier(id, escape_char='_'):
    """Unescape identifier.  Satisfies ``s ==
    unescape_identifier(escape_identifier(s))`` for all strings ``s``.

    :param str id: String to unescape.
    :param str escape_char: Escape character.

    :return: unescaped string.
    :rtype: str

    """
    
    return Env.hail().utils.StringEscapeUtils.unescapeIdentifier(escaped_id, escape_char)
