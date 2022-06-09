import 'package:test/test.dart';
import 'package:redis_dart/redis_dart.dart';

void main() {
  late RedisClient client;

  setUp(() async {
    client = await RedisClient.connect('localhost');
  });

  tearDown(() async {
    await client.close();
  });

  test('Connection test', () async {
    var res = await client.get('key');

    expect(res.value, equals(null));
  });

  test('Set test', () async {
    var res = await client.set('key', 'value');

    expect(res.value, equals('OK'));
    await client.delete('key');
  });

  test('Get test', () async {
    await client.set('key', 'value');
    var res = await client.get('key');

    expect(res.value, equals('value'));
    await client.delete('key');
  });

  test('List test', () async {
    await client.pushLast('a', 0);
    var res = await client.popFirst('a');

    expect(res.value, equals('0'));
    await client.delete('a');
  });

  test('Hash test', () async {
    await client.setMap('map', {'two': 2});
    var res = await client.getMap('map');

    expect(int.parse(res.value['two']), equals(2));
    await client.delete('map');
  });

  test('Pub/sub works', () async {
    const channels = ['test'];
    var count = 0;
    final subClient = await RedisClient.connect('localhost');
    final stream = await subClient.subscribe(channels);
    final sub = stream.listen((_) => count++);

    await client.publish(channel: 'test', message: 'hi');
    await client.publish(channel: 'test', message: 'there');

    // Need to wait a bit for all the messages to be received.
    await Future.delayed(const Duration(milliseconds: 50));

    await subClient.unsubscribe(channels);
    await subClient.close();
    await sub.cancel();

    expect(count, equals(2));
  });
}
