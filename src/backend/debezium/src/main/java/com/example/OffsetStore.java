package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.LinkOption;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Base64;
import java.security.MessageDigest;
import java.util.HexFormat;

/* Debezium's FileOffsetBackingStore persists a serialized HashMap<byte[],byte[]>.
 * Preserve that format while validating it before exposing its contents. */
public final class OffsetStore
{
	private static final ObjectMapper JSON = new ObjectMapper()
		.enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
		.enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
	private static final long MAX_FILE_BYTES = 64L * 1024 * 1024;
	private static final long MAX_ENTRIES = 1_000_000;

	private OffsetStore() {}

	/* A standalone maintenance JVM loads only this codec, not a Debezium
	 * engine. Updates require the operator's hash from an offline inspection. */
	public static void main(String[] arguments) throws Exception
	{
		if (arguments.length == 2 && arguments[0].equals("inspect"))
		{
			Path path = Path.of(arguments[1]);
			Map<ByteBuffer, ByteBuffer> entries = read(path.toFile());
			ObjectNode result = JSON.createObjectNode();
			result.put("format", "debezium-file-offset-v1");
			result.put("sha256", digest(path));
			ArrayNode values = result.putArray("entries");
			for (Map.Entry<ByteBuffer, ByteBuffer> entry : entries.entrySet())
			{
				ObjectNode value = values.addObject();
				value.put("key_base64", Base64.getEncoder().encodeToString(entry.getKey().array()));
				if (entry.getValue() == null)
					value.putNull("value_base64");
				else
					value.put("value_base64", Base64.getEncoder().encodeToString(entry.getValue().array()));
			}
			System.out.println(JSON.writeValueAsString(result));
			return;
		}
		if (arguments.length == 6 && arguments[0].equals("set-offline"))
		{
			Path path = Path.of(arguments[1]);
			String expected = arguments[5];
			if (!expected.matches("[0-9a-f]{64}") || !digest(path).equals(expected))
				throw new IllegalStateException("Offline offset hash does not match the inspected file");
			int type = Integer.parseInt(arguments[2]);
			/* A second hash closes accidental edits between inspection and
			 * decoding. The connector must remain stopped throughout maintenance. */
			Map<ByteBuffer, ByteBuffer> entries = read(path.toFile());
			if (!digest(path).equals(expected))
				throw new IllegalStateException("Offset file changed during offline inspection");
			requireOffsetValue(arguments[4]);
			ByteBuffer previous = partitionMapKey(entries, type, arguments[3]);
			if (previous == null)
				throw new IllegalStateException("Offset update requires an existing connector partition");
			entries.remove(previous);
			entries.put(ByteBuffer.wrap(partitionKey(type, arguments[3])), ByteBuffer.wrap(utf8(arguments[4])));
			write(path.toFile(), raw(entries));
			System.out.println(digest(path));
			return;
		}
		throw new IllegalArgumentException("Usage: inspect FILE | set-offline FILE TYPE DATABASE VALUE EXPECTED_SHA256 (connector stopped)");
	}

	private static String digest(Path path) throws Exception
	{
		if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > MAX_FILE_BYTES)
			throw new IOException("Offset path must be a regular file no larger than 64 MiB");
		return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
	}

	private static String connectorPrefix(int type)
	{
		switch (type)
		{
			case 1: return "mysql";
			case 2: return "oracle";
			case 3: return "sqlserver";
			case 5: return "postgres";
			default: throw new IllegalArgumentException("Unsupported connector type: " + type);
		}
	}

	private static byte[] partitionKey(int type, String database)
	{
		connectorPrefix(type);
		ArrayNode key = JSON.createArrayNode().add("engine");
		ObjectNode partition = JSON.createObjectNode().put("server", "synchdb-connector");
		if (type == 3)
		{
			if (sqlServerDatabases(database).size() != 1)
				throw new IllegalArgumentException("SQL Server offset updates require one explicit database partition");
			/* Validate scalar values before Jackson escapes the JSON string. */
			utf8(database);
			partition.put("database", database);
		}
		key.add(partition);
		try
		{
			return JSON.writeValueAsBytes(key);
		}
		catch (IOException failure)
		{
			throw new IllegalArgumentException("Cannot encode Debezium offset partition", failure);
		}
	}

	/* Match the pinned connector's comma-separated, case-sensitive partition
	 * names. A database list is never itself an offset partition. */
	private static List<String> sqlServerDatabases(String database)
	{
		if (database == null)
			throw new IllegalArgumentException("SQL Server offset requires a database name");
		List<String> databases = Arrays.asList(database.split(",", -1));
		HashSet<String> unique = new HashSet<>();
		for (String name : databases)
		{
			utf8(name);
			if (name.isBlank() || !unique.add(name))
				throw new IllegalArgumentException("SQL Server offset requires distinct non-empty database names");
		}
		return databases;
	}

	private static JsonNode requireOffsetValue(String value)
	{
		return offsetValue(value, true);
	}

	private static JsonNode offsetValue(String value, boolean requirePosition)
	{
		utf8(value);
		try
		{
			JsonNode offset = JSON.readTree(value);
			if (offset == null || !offset.isObject() || (requirePosition && offset.isEmpty()))
				throw new IllegalArgumentException("Offset value must be a non-empty JSON object");
			return offset;
		}
		catch (IOException failure)
		{
			throw new IllegalArgumentException("Offset value must be a valid non-empty JSON object with unique keys", failure);
		}
	}

	private static byte[] utf8(String value)
	{
		if (value == null)
			throw new IllegalArgumentException("Offset text must not be null");
		try
		{
			ByteBuffer bytes = StandardCharsets.UTF_8.newEncoder()
				.onMalformedInput(CodingErrorAction.REPORT)
				.onUnmappableCharacter(CodingErrorAction.REPORT)
				.encode(java.nio.CharBuffer.wrap(value));
			byte[] copy = new byte[bytes.remaining()];
			bytes.get(copy);
			return copy;
		}
		catch (CharacterCodingException failure)
		{
			throw new IllegalArgumentException("Offset text contains an invalid Unicode scalar value", failure);
		}
	}

	static String get(int type, String database, String name, String destination)
	{
		Path path = Path.of("pg_synchdb", connectorPrefix(type) + "_" + name + "_" + destination + "_offsets.dat");
		return getFile(type, database, path);
	}

	static String getFile(int type, String database, Path path)
	{
		if (Files.notExists(path))
			return "offset file not flushed yet";
		Map<ByteBuffer, ByteBuffer> entries = read(path.toFile());
		if (type == 3 && sqlServerDatabases(database).size() > 1)
		{
			ObjectNode result = JSON.createObjectNode();
			ObjectNode partitions = result.putObject("partitions");
			boolean completed = true;
			for (String name : sqlServerDatabases(database))
			{
				ByteBuffer value = partitionValue(entries, type, name);
				JsonNode offset = value == null ? null : offsetValue(offsetText(value), false);
				partitions.set(name, offset);
				completed &= snapshotCompleted(offset);
			}
			/* C observes this top-level marker before inspecting individual
			 * commit_lsn fields; one completed database cannot finish the rest. */
			result.put("snapshot_completed", completed);
			return result.toString();
		}
		ByteBuffer value = partitionValue(entries, type, database);
		if (value == null)
			return "NULL";
		return offsetText(value);
	}

	private static boolean snapshotCompleted(JsonNode offset)
	{
		if (offset == null)
			return false;
		JsonNode completed = offset.get("snapshot_completed");
		if (completed != null)
			return completed.isBoolean() && completed.booleanValue();
		JsonNode snapshot = offset.get("snapshot");
		if (snapshot != null && (!snapshot.isBoolean() || snapshot.booleanValue()))
			return false;
		return offset.path("commit_lsn").isTextual();
	}

	private static String offsetText(ByteBuffer value)
	{
		try
		{
			return StandardCharsets.UTF_8.newDecoder()
				.onMalformedInput(CodingErrorAction.REPORT)
				.onUnmappableCharacter(CodingErrorAction.REPORT)
				.decode(value.duplicate()).toString();
		}
		catch (CharacterCodingException failure)
		{
			throw new IllegalStateException("Debezium offset value is not valid UTF-8", failure);
		}
	}

	static void requireResumePartition(int type, String database, Path path)
	{
		if (Files.notExists(path))
			return; /* Only the C ownership gate can authorize a fresh generation. */
		Map<ByteBuffer, ByteBuffer> entries = read(path.toFile());
		if (type == 3)
		{
			for (String name : sqlServerDatabases(database))
				requireResumePartition(entries, type, name);
		}
		else
			requireResumePartition(entries, type, database);
	}

	private static void requireResumePartition(Map<ByteBuffer, ByteBuffer> entries, int type, String database)
	{
		ByteBuffer key = partitionMapKey(entries, type, database);
		if (key != null && !key.equals(ByteBuffer.wrap(partitionKey(type, database))))
			throw new IllegalStateException("Offset partition encoding is not compatible with the pinned Kafka converter; normalize it using offline maintenance");
		ByteBuffer value = key == null ? null : entries.get(key);
		if (value == null)
			throw new IllegalStateException("Engine-owned offset has no usable connector partition");
		try
		{
			requireOffsetValue(offsetText(value));
		}
		catch (IllegalArgumentException failure)
		{
			throw new IllegalStateException("Engine-owned offset is not a valid non-empty JSON object", failure);
		}
	}

	private static ByteBuffer partitionValue(Map<ByteBuffer, ByteBuffer> entries, int type, String database)
	{
		ByteBuffer key = partitionMapKey(entries, type, database);
		return key == null ? null : entries.get(key);
	}

	private static ByteBuffer partitionMapKey(Map<ByteBuffer, ByteBuffer> entries, int type, String database)
	{
		try
		{
			com.fasterxml.jackson.databind.JsonNode expected = JSON.readTree(partitionKey(type, database));
			ByteBuffer found = null;
			for (ByteBuffer key : entries.keySet())
			{
				com.fasterxml.jackson.databind.JsonNode candidate;
				try
				{
					String text = StandardCharsets.UTF_8.newDecoder()
						.onMalformedInput(CodingErrorAction.REPORT)
						.onUnmappableCharacter(CodingErrorAction.REPORT).decode(key.duplicate()).toString();
					candidate = JSON.readTree(text);
				}
				catch (IOException unrelated)
				{
					continue; /* Other opaque FileOffsetBackingStore partitions survive edits. */
				}
				if (expected.equals(candidate))
				{
					if (found != null)
						throw new IllegalStateException("Ambiguous engine offset partition");
					found = key;
				}
			}
			return found;
		}
		catch (IOException failure)
		{
			throw new IllegalStateException("Cannot decode engine offset partition", failure);
		}
	}

	static void set(String filename, int type, String database, String value)
	{
		byte[] key = partitionKey(type, database);
		requireOffsetValue(value);
		byte[] bytes = utf8(value);
		File file = new File(filename);
		/* A missing file is an error: the caller must not report a successful update. */
		Map<ByteBuffer, ByteBuffer> entries = read(file);
		ByteBuffer previous = partitionMapKey(entries, type, database);
		if (previous == null)
			throw new IllegalStateException("Offset update requires an existing connector partition");
		entries.remove(previous);
		entries.put(ByteBuffer.wrap(key), ByteBuffer.wrap(bytes));
		write(file, raw(entries));
	}

	static void create(String filename, int type, String database, String value)
	{
		Map<byte[], byte[]> entries = new HashMap<>();
		entries.put(partitionKey(type, database), utf8(value));
		write(new File(filename), entries);
	}

	private static ObjectInputFilter.Status inputFilter(ObjectInputFilter.FilterInfo info)
	{
		if (info.depth() > 32 || info.references() > 4 * MAX_ENTRIES ||
			info.streamBytes() > MAX_FILE_BYTES || info.arrayLength() > MAX_FILE_BYTES)
			return ObjectInputFilter.Status.REJECTED;
		Class<?> type = info.serialClass();
		if (type == null)
			return ObjectInputFilter.Status.UNDECIDED;
		/* HashMap rounds its table capacity up to a power of two. */
		if (type == Map.Entry[].class && info.arrayLength() > 4 * MAX_ENTRIES)
			return ObjectInputFilter.Status.REJECTED;
		return type == HashMap.class || type == LinkedHashMap.class ||
			type == byte[].class || type == Map.Entry[].class ?
			ObjectInputFilter.Status.ALLOWED : ObjectInputFilter.Status.REJECTED;
	}

	static Map<ByteBuffer, ByteBuffer> read(File file)
	{
		try
		{
			if (!Files.isRegularFile(file.toPath(), LinkOption.NOFOLLOW_LINKS) || Files.size(file.toPath()) > MAX_FILE_BYTES)
				throw new IOException("Debezium offset file exceeds 64 MiB");
			try (ObjectInputStream input = new ObjectInputStream(Files.newInputStream(file.toPath(), LinkOption.NOFOLLOW_LINKS)))
			{
				input.setObjectInputFilter(OffsetStore::inputFilter);
				Object object = input.readObject();
				if (!(object instanceof Map<?, ?>))
					throw new IllegalStateException("Debezium offset file must contain a map");
				Map<ByteBuffer, ByteBuffer> entries = validated((Map<?, ?>) object);
				if (input.read() != -1)
					throw new IOException("Trailing bytes after Debezium offset map");
				return entries;
			}
		}
		catch (IOException | ClassNotFoundException failure)
		{
			throw new IllegalStateException("Cannot read Debezium offset file " + file, failure);
		}
	}

	private static Map<ByteBuffer, ByteBuffer> validated(Map<?, ?> entries)
	{
		if (entries.size() > MAX_ENTRIES)
			throw new IllegalArgumentException("Too many Debezium offset entries");
		Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
		long payloadBytes = 0;
		for (Map.Entry<?, ?> entry : entries.entrySet())
		{
			if (!(entry.getKey() instanceof byte[]) ||
				(entry.getValue() != null && !(entry.getValue() instanceof byte[])))
				throw new IllegalArgumentException("Debezium offsets require byte[] keys and byte[] or null values");
			byte[] key = (byte[]) entry.getKey();
			byte[] value = (byte[]) entry.getValue();
			payloadBytes += (long) key.length + (value == null ? 0 : value.length);
			if (payloadBytes > MAX_FILE_BYTES)
				throw new IllegalArgumentException("Debezium offset payload exceeds 64 MiB");
			ByteBuffer contentKey = ByteBuffer.wrap(key.clone());
			if (result.containsKey(contentKey))
				throw new IllegalArgumentException("Duplicate Debezium offset key bytes");
			result.put(contentKey, value == null ? null : ByteBuffer.wrap(value.clone()));
		}
		return result;
	}

	private static Map<byte[], byte[]> raw(Map<ByteBuffer, ByteBuffer> entries)
	{
		Map<byte[], byte[]> result = new HashMap<>();
		for (Map.Entry<ByteBuffer, ByteBuffer> entry : entries.entrySet())
			result.put(entry.getKey().array(), entry.getValue() == null ? null : entry.getValue().array());
		return result;
	}

	static void write(File file, Map<byte[], byte[]> entries)
	{
		/* Validate and copy before creating a temporary file or touching the destination. */
		Map<byte[], byte[]> snapshot = raw(validated(entries));
		/* Keep an ASCII relative engine path relative: converting the data
		 * directory through a JVM's native filename encoding is unnecessary. */
		Path destination = file.toPath().normalize();
		Path parent = destination.getParent() == null ? Path.of(".") : destination.getParent();
		Path temporary = null;
		Throwable writeFailure = null;
		try
		{
			temporary = Files.createTempFile(parent, "." + destination.getFileName() + "-", ".tmp");
			try (ObjectOutputStream output = new ObjectOutputStream(Files.newOutputStream(temporary)))
			{
				output.writeObject(snapshot);
			}
			if (Files.size(temporary) > MAX_FILE_BYTES)
				throw new IOException("Debezium offset file exceeds 64 MiB");
			try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE))
			{
				channel.force(true);
			}
			/* Each file is replaced atomically; the catalog gates multi-file
			 * publication. A directory sync failure remains visible to maintenance. */
			Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
			temporary = null;
			try (FileChannel directory = FileChannel.open(parent, StandardOpenOption.READ))
			{
				directory.force(true);
			}
		}
		catch (IOException failure)
		{
			writeFailure = new IllegalStateException("Cannot write Debezium offset file " + file, failure);
			throw (IllegalStateException) writeFailure;
		}
		catch (RuntimeException | Error failure)
		{
			writeFailure = failure;
			throw failure;
		}
		finally
		{
			if (temporary != null)
			{
				try
				{
					Files.deleteIfExists(temporary);
				}
				catch (IOException failure)
				{
					/* Preserve the write failure while making cleanup failure visible. */
					if (writeFailure != null)
						writeFailure.addSuppressed(failure);
					else
						throw new IllegalStateException("Cannot remove temporary Debezium offset file " + temporary, failure);
				}
			}
		}
	}
}
