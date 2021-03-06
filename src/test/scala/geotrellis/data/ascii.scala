package geotrellis.data

import geotrellis._
import geotrellis.raster._

import geotrellis._
import geotrellis.testutil._

import org.scalatest.FunSpec
import org.scalatest.matchers.MustMatchers
import org.scalatest.matchers.ShouldMatchers

class AsciiSpec extends FunSpec 
                   with MustMatchers 
                   with ShouldMatchers {
  val data:Array[Int] = (1 to 100).toArray

  val e = Extent(19.0, 9.0, 49.0, 39.0)
  val g = RasterExtent(e, 3.0, 3.0, 10, 10)
  val r = Raster(data, g)

  describe("An AsciiReader") {
    it ("should fail on non-existent files") {
      val path = "/does/not/exist.tif"
      evaluating { AsciiRasterLayerBuilder.fromFile(path) } should produce [Exception]
    }

    it ("should write ASCII") {
      AsciiWriter.write("/tmp/foo.asc", r, "foo")
    }

    it ("should read ASCII") {
      val r2 = AsciiRasterLayerBuilder.fromFile("/tmp/foo.asc").getRaster()
      r2 must be === r
    }

  }
}
