import 'dart:async';
import 'dart:typed_data';
import 'dart:convert';
import '../resp/resp_object.dart';
import '../resp/resp_parser.dart';

_PacketBuffer? _bulkStringBuffer;
_PacketBuffer? _listBuffer;

class _PacketBuffer {
  final _buffer = <int>[];
  var _currentSize = 0;
  final int _totalSize;

  _PacketBuffer({required int totalSize}) : _totalSize = totalSize;

  void add(Uint8List bytes) {
    _buffer.addAll(bytes);
    _currentSize += bytes.lengthInBytes;
  }

  bool get complete => _currentSize >= _totalSize;

  List<int> get bytes => _buffer;

  void clear() => _buffer.clear();
}

final _parser = RespParser();
final respDecoder = StreamTransformer<Uint8List, RespObject>.fromHandlers(
  handleData: _handleData,
  handleError: (err, st, sink) => sink.addError(err),
  handleDone: (sink) => sink.close(),
);

void _handleData(Uint8List event, EventSink<RespObject> sink) {

  // If buffer is not empty, this packet is a continuation of a bulk string.
  final bulkStringBuffer = _bulkStringBuffer;
  if (bulkStringBuffer != null) {
    bulkStringBuffer.add(event);
    if (!bulkStringBuffer.complete) return;

    final content = utf8.decode(bulkStringBuffer.bytes);
    try {
      sink.add(_parser.parse(content));
    } catch (_) {
      print('error parsing bulk string: $content');
      rethrow;
    } finally {
      bulkStringBuffer.clear();
      _bulkStringBuffer = null;
    }
  }

  final listBuffer = _listBuffer;
  if (listBuffer != null) {
    listBuffer.add(event);

    try {
      final content = utf8.decode(listBuffer.bytes);
      final o = _parser.parse(content);

      listBuffer.clear();
      _listBuffer = null;

      sink.add(o);
      return;
    } on RangeError catch (_) {
      // Expected, nothing to do.
      return;
    } catch (e) {
      final buff = StringBuffer('error on list buffer: $e');
      try {
        final content = utf8.decode(listBuffer.bytes);
        buff.write(', content: $content');
      } catch (e) {
        buff.write(', error decoding content: $e');
      }
      print(buff);

      rethrow;
    }
  }

  final s = utf8.decode(event);

  // Multi-bulk string, may need to buffer.
  if (s.startsWith(r'$')) {
    final i = s.indexOf('\r\n', 1);
    final n = s.substring(1, i);
    final len = int.parse(n);

    // Special case for null bulk string.
    if (len == -1) {
      sink.add(_parser.parse(s));
      return;
    }

    final totalSize = r'$'.length + len + '\r\n'.length;
    final rest = s.substring(i + '\r\n'.length);
    if ((rest.length - '\r\n'.length) < len) {
      // Need to buffer
      _bulkStringBuffer = _PacketBuffer(totalSize: totalSize)..add(event);
      return;
    }
  // List, may need to buffer
  } else if (s.startsWith('*')) {

    // Try parsing first, if ok, nothing to do.
    // If RangeError occurs, then we need to buffer.
    try {
      sink.add(_parser.parse(s));
      return;
    } on RangeError catch (_) {
      _listBuffer = _PacketBuffer(totalSize: 512 * 1024 * 1024)..add(event); // Max size is 512MB.
      return;
    }
  }

  sink.add(_parser.parse(s));
}

List<RespBulkString> mapBulkStrings(List<String> strs) {
  return strs.map((e) => RespBulkString(e)).toList();
}

Map? listToMap(List? list) {
  if (list == null) {
    return null;
  }
  final keys = [];
  final values = [];
  for (var i = 0; i < list.length; i++) {
    if (i.isEven) {
      keys.add(list[i]);
    } else {
      values.add(list[i]);
    }
  }
  return {
    for (var i = 0; i < keys.length; i++) keys[i]: values[i],
  };
}

List<RespObject> mapToList(Map map) {
  final list = <RespBulkString>[];
  for (var key in map.keys) {
    list.add(RespBulkString(key));
    list.add(RespBulkString(map[key]));
  }
  return list;
}
