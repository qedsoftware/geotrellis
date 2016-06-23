package geotrellis.process

import scala.collection.mutable
import java.io.File
import geotrellis._
import geotrellis.util.Filesystem

import scala.util._

import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * Represents a location where data can be loaded from (e.g. the filesystem,
 * postgis, a web service, etc).
 */
case class DataStore(name:String, params:Map[String, String],catalogPath:String) {

  private val layers = mutable.Map.empty[String, RasterLayer]

  private val rwLock = new ReentrantReadWriteLock()

  initRasterLayers()

  /**
   * Initialize raster layers from the directory specified by the 'path' param.
   */
  private def initRasterLayers() {
    val path = params("path")
    // Make relative paths relative to the catalog path.

    val f = {
      val f = new File(path)
      if(f.isAbsolute) f
      else new File(new File(catalogPath).getParentFile,path)
    }

    if (!f.isDirectory) {
      sys.error("store %s is not a directory" format path)
    }

    // Walk the directory to for raster layers;
    // also search subdirectories, but some directories
    // might be tiled rasters.
    initDirectory(f)
  }

  private def initDirectory(d:File) {
    val skipDirectories = mutable.Set[String]()
    for(f <- d.listFiles
              .filter(_.isFile)
              .filter(_.getPath.endsWith(".json"))) {
      // It's a JSON file
      // which may contain layer metadata,
      // or we just ignore it.
      RasterLayer.fromFile(f) match {
        case Success(layer) =>
          layers(layer.info.id.name) = layer
          // Skip the tile directory if it's a tiled raster.
          layer match {
            case tl:TileSetRasterLayer =>
              skipDirectories.add(new File(tl.tileDirPath).getAbsolutePath)
            case _ =>
          }
        case Failure(e) =>
          System.err.println(s"[ERROR] Skipping ${f.getPath}: $e")
      }
    }

    // Recurse through subdirectories. If a directory was marked
    // as containing a tile set, skip it.
    for(subdir <- d.listFiles
                   .filter(_.isDirectory)
                   .filter(f => !skipDirectories.contains(f.getAbsolutePath))) {
      initDirectory(subdir)
    }
  }

  /**
   * Sets the cache for all child layers.
   */
  def setCache(c:Option[Cache[String]]) = {
    for(layer <- layers.values) { layer.setCache(c) }
  }

  def cacheAll() =
    layers.values
          .map(_.cache)

  def getNames = layers.keys
  def getLayers = layers.values

  def hasCacheAll = if(params.contains("cacheAll")) {
    val value = params("cacheAll").toLowerCase
    value == "true" || value == "yes" || value == "1"
  } else { false }

  def getRasterLayer(name:String):Option[RasterLayer] = {

    rwLock.readLock().lock()
    val layer = layers.get(name) match {

      case Some(foundLayer)=>
        rwLock.readLock().unlock()
        Some(foundLayer)

      case None=>
        rwLock.readLock().unlock()

        rwLock.writeLock().lock()

        // Needs to ensure that layer is not loaded already by another thread
        // before we actually acquired write lock
        layers.get(name) match {

          case Some(foundLayer) =>
            rwLock.writeLock().unlock()
            Some(foundLayer)

          case None=>
            val path = params("path")
            val fullFileName = s"$path/$name.json"

            System.err.println(s"[NOTICE] Layer not found in cache. Try to load dynamically from $fullFileName")

            try {

              RasterLayer.fromFile(new File(fullFileName)) match {

                case Success(readedLayer) =>

                  System.err.println(s"[NOTICE] Layer $name loaded successfully from $fullFileName")

                  layers(name) = readedLayer
                  Some(readedLayer)

                case Failure(e) =>
                  System.err.println(s"[ERROR] Can't load layer $name: $e")
                  None
              }

            } catch {
              case e:Exception=>
                e.printStackTrace()
                None
            } finally {
              rwLock.writeLock().unlock()
            }
        }
    }

    layer
  }
}
