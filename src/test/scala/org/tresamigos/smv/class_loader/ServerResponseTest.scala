package org.tresamigos.smv.class_loader

import java.io.ByteArrayOutputStream

import org.tresamigos.smv.SparkTestUtil

class ServerResponseTest extends SparkTestUtil {
  private val apiVersionBytes = Array[Byte](0, 0,0,ServerResponse.apiVersion.toByte)

  test("test OK Server Response Marshall") {
    val okResp = new ServerResponse(20, Array[Byte](1,2,3))

    val baos = new ByteArrayOutputStream()
    okResp.send(baos)

    val expectedRawBytes = apiVersionBytes ++ Array[Byte](
      0,0,0,ServerResponse.STATUS_OK.toByte,  // status
      0,0,0,0,0,0,0,20,                       // class version
      0,0,0,3,                                // num of bytes
      1,2,3                                   // class bytes
    )
    assert(baos.toByteArray === expectedRawBytes)
  }

  test("test OK Server Response De-Marshall") {
    val rawBytes = apiVersionBytes ++ Array[Byte](
      0,0,0,ServerResponse.STATUS_OK.toByte,  // status
      0,0,0,0,0,0,0,32,                       // class version
      0,0,0,4,                                // num of bytes
      1,2,3,4                                 // class bytes
    )
    val resp = ServerResponse(rawBytes)

    assert(resp.status === ServerResponse.STATUS_OK)
    assert(resp.classVersion === 32L)
    assert(resp.classBytes === Array[Byte](1,2,3,4))
  }

  test("test ERROR Server Response Marshall") {
    val errResp = new ServerResponse(ServerResponse.STATUS_ERR_CLASS_NOT_FOUND)

    val baos = new ByteArrayOutputStream()
    errResp.send(baos)

    val expectedRawBytes = apiVersionBytes ++ Array[Byte](
      0,0,0,ServerResponse.STATUS_ERR_CLASS_NOT_FOUND.toByte
    )
    assert(baos.toByteArray === expectedRawBytes)

  }

  test("test ERROR server Response De-Marshall") {
    val rawBytes = apiVersionBytes ++ Array[Byte](
      0,0,0,ServerResponse.STATUS_ERR_CLASS_NOT_FOUND.toByte)
    val resp = ServerResponse(rawBytes)

    assert(resp.status === ServerResponse.STATUS_ERR_CLASS_NOT_FOUND)
    assert(resp.classVersion === 0L)
    assert(resp.classBytes === null)

  }
}
