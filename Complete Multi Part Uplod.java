
/**
 * Class to test Multipart upload end to end.
 */

public class TestMultipartUploadComplete {

  private static final ObjectEndpoint REST = new ObjectEndpoint();
  private static final OzoneClient CLIENT = new OzoneClientStub();

  @BeforeClass
  public static void setUp() throws Exception {

    CLIENT.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);


    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    REST.setHeaders(headers);
    REST.setClient(CLIENT);
    REST.setOzoneConfiguration(new OzoneConfiguration());
  }

 
  // METHOD TO INITIATE A MULTI-PART REQUESGT 
  private String initiateMultipartUpload(String key) throws IOException,
      OS3Exception {
    // Object endpoint method initializeMultipartUpload to Initiate the Multipart response
    Response response = REST.initializeMultipartUpload(OzoneConsts.S3_BUCKET,
        key);
        
    // Creating an object for the Multipart-Upload-Initiate RESPONSE
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
        
    // FETCHING THE UPLOAD ID FROM THE RESPONSE
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    return uploadID;
  }

  // METHOD TO UPLOAD A SINGLE PART - (Will be called for uploading each part)
  private Part uploadPart(String key, String uploadID, int partNumber, String
      content) throws IOException, OS3Exception {
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    // Key Endpoint put() to upload the part using the part number 
    Response response = REST.put(OzoneConsts.S3_BUCKET, key, content.length(),
        partNumber, uploadID, body);
    Part part = new Part();
    part.seteTag(response.getHeaderString("ETag"));
    part.setPartNumber(partNumber);

    return part;
  }

  // METHOD TO COMPLETE THE PART AFTER ALL THE PARTS HAVE BEEN UPLOADED 
  private void completeMultipartUpload(String key,
      CompleteMultipartUploadRequest completeMultipartUploadRequest,
      String uploadID) throws IOException, OS3Exception {
    Response response = REST.completeMultipartUpload(OzoneConsts.S3_BUCKET, key,
        uploadID, completeMultipartUploadRequest);

    assertEquals(200, response.getStatus());

    CompleteMultipartUploadResponse completeMultipartUploadResponse =
        (CompleteMultipartUploadResponse) response.getEntity();

    assertEquals(OzoneConsts.S3_BUCKET,
        completeMultipartUploadResponse.getBucket());
    assertEquals(key, completeMultipartUploadResponse.getKey());
    assertEquals(OzoneConsts.S3_BUCKET,
        completeMultipartUploadResponse.getLocation());
    assertNotNull(completeMultipartUploadResponse.getETag());
  }

  @Test
  public void testMultipart() throws Exception {

    // Initiate multipart upload
    String uploadID = initiateMultipartUpload(OzoneConsts.KEY);

    List<Part> partsList = new ArrayList<>();


    // UPLOADING PART 1
    String content = "Multipart Upload 1";
    int partNumber = 1;

    Part part1 = uploadPart(OzoneConsts.KEY, uploadID, partNumber, content);
    partsList.add(part1);
    
    // UPLOADING PART 2
    content = "Multipart Upload 2";
    partNumber = 2;
    Part part2 = uploadPart(OzoneConsts.KEY, uploadID, partNumber, content);
    partsList.add(part2);

    // complete multipart upload
    CompleteMultipartUploadRequest completeMultipartUploadRequest = new
        CompleteMultipartUploadRequest();
    completeMultipartUploadRequest.setPartList(partsList);


    completeMultipartUpload(OzoneConsts.KEY, completeMultipartUploadRequest,
        uploadID);

  }


  @Test
  public void testMultipartInvalidPartOrderError() throws Exception {

    // Initiate multipart upload
    String key = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(key);

    List<Part> partsList = new ArrayList<>();

    // Upload parts
    String content = "Multipart Upload 1";
    int partNumber = 1;

    Part part1 = uploadPart(key, uploadID, partNumber, content);
    // Change part number
    part1.setPartNumber(3);
    partsList.add(part1);

    content = "Multipart Upload 2";
    partNumber = 2;

    Part part2 = uploadPart(key, uploadID, partNumber, content);
    partsList.add(part2);

    // complete multipart upload
    CompleteMultipartUploadRequest completeMultipartUploadRequest = new
        CompleteMultipartUploadRequest();
    completeMultipartUploadRequest.setPartList(partsList);
    try {
      completeMultipartUpload(key, completeMultipartUploadRequest, uploadID);
      fail("testMultipartInvalidPartOrderError");
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.INVALID_PART_ORDER.getCode(), ex.getCode());
    }
  }
  
}
