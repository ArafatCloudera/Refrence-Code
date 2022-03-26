 1.  Creating a random key and storing it inside an exisiting bucket 
 
    // Creating a random file/key
    String value = RandomStringUtils.randomAlphanumeric(32);
    // Writing the file contents inside the bucket 
    OzoneOutputStream out = bucket.createKey("key1",
        value.getBytes(UTF_8).length, ReplicationType.RATIS,
        ReplicationFactor.ONE, new HashMap<>());
    // Writing the file into the Output Stream
    out.write(value.getBytes(UTF_8));
    out.close();
