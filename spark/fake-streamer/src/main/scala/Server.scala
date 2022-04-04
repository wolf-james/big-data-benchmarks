// Simple server
import java.net.ServerSocket
import java.io.{BufferedReader, InputStreamReader, PrintStream}
import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicSessionCredentials}
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest


object Server extends App {
  val BUCKET_NAME = ""
  val FILE_NAME = ""

  def getIAMRoleCredentials = {
    val AWS_ROLE_ARN = sys.env.getOrElse("AWS_ROLE_ARN", "")
    val AWS_ROLE_SESSION_NAME = sys.env.getOrElse("AWS_ROLE_SESSION_NAME", "")

    val stsClient = AWSSecurityTokenServiceClientBuilder.standard()
      .withCredentials(new ProfileCredentialsProvider(""))
      .build()

    val roleRequest = new AssumeRoleRequest()
      .withRoleArn(AWS_ROLE_ARN)
      .withRoleSessionName(AWS_ROLE_SESSION_NAME)
    val roleResponse = stsClient.assumeRole(roleRequest)

    val sessionCredentials = roleResponse.getCredentials
    new BasicSessionCredentials(
      sessionCredentials.getAccessKeyId,
      sessionCredentials.getSecretAccessKey,
      sessionCredentials.getSessionToken
    )
  }

  def getBasicCredentials = {
    new BasicSessionCredentials(
      sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""),
      sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""),
      sys.env.getOrElse("AWS_SESSION_TOKEN", ""),
    )
  }

  def getS3Data: BufferedReader = {
    try {
      val awsS3Client = AmazonS3ClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(getIAMRoleCredentials))
        .build()

      val obj = awsS3Client.getObject(BUCKET_NAME, FILE_NAME)
      new BufferedReader(new InputStreamReader(obj.getObjectContent))
    } catch {
      case ase: AmazonServiceException => System.err.println("Exception: " + ase.toString); return null
      case ace: AmazonClientException => System.err.println("Exception: " + ace.toString); return null
    }
  }

  val server = new ServerSocket(9999)
  while (true) {
    val s = server.accept()
    val out = new PrintStream(s.getOutputStream)

    // Read data from S3
    val br = getS3Data
    var line = br.readLine
    while (line != null) {
      out.println(line)
      out.flush()
      line = br.readLine
    }
    br.close()

    s.close()
  }
}
