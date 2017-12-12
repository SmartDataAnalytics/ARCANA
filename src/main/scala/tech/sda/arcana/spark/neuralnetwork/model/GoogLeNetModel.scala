package tech.sda.arcana.spark.neuralnetwork.model
import com.intel.analytics.bigdl.numeric.NumericFloat
import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl.utils.{T, Table}
import com.intel.analytics.bigdl.Module
import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.nn.{Graph, _}
import com.intel.analytics.bigdl._

/*
 * Sequential[e5fa8b85]{
  [input -> (1) -> (2) -> output]
  (1): Sequential[3142c3f7]{
    [input -> (1) -> (2) -> (3) -> (4) -> (5) -> (6) -> (7) -> (8) -> (9) -> (10) -> (11) -> (12) -> output]
    (1): SpatialZeroPadding[a2919da7](l=0, r=214, t=0, b=214)
    (2): Sequential[6833eee2]{
      [input -> (1) -> (2) -> (3) -> (4) -> output]
      (1): nn.ParallelTable {
	input
	  |`-> (1): Sequential[5c9a02d4]{
	  |      [input -> (1) -> (2) -> (3) -> output]
	  |      (1): nn.Contiguous
	  |      (2): View[ee898519](-1x1x224x224)
	  |      (3): SpatialConvolution[22c78f6c](1 -> 8, 7 x 7, 2, 2, 3, 3)
	  |    }
	  |`-> (2): Sequential[5c9a02d4]{
	  |      [input -> (1) -> (2) -> (3) -> output]
	  |      (1): nn.Contiguous
	  |      (2): View[ee898519](-1x1x224x224)
	  |      (3): SpatialConvolution[22c78f6c](1 -> 8, 7 x 7, 2, 2, 3, 3)
	  |    }
	   `-> (3): Sequential[5c9a02d4]{
	         [input -> (1) -> (2) -> (3) -> output]
	         (1): nn.Contiguous
	         (2): View[ee898519](-1x1x224x224)
	         (3): SpatialConvolution[22c78f6c](1 -> 8, 7 x 7, 2, 2, 3, 3)
	       }
	   ... -> output
}
      (2): ReLU[34f3bbb](0.0, 0.0)
      (3): SpatialConvolution[fbef2bb8](24 -> 64, 1 x 1, 1, 1, 0, 0)
      (4): ReLU[fc73fbf4](0.0, 0.0)
    }
    (3): SpatialMaxPooling[afe8e105](3, 3, 2, 2, 0, 0)
    (4): SpatialConvolution[3bf93825](64 -> 64, 1 x 1, 1, 1, 0, 0)
    (5): ReLU[c204407b](0.0, 0.0)
    (6): SpatialConvolution[f03c04ba](64 -> 192, 3 x 3, 1, 1, 1, 1)
    (7): ReLU[44db75b5](0.0, 0.0)
    (8): SpatialMaxPooling[aac77188](3, 3, 2, 2, 0, 0)
    (9): Concat[aff6e045]{
      input
        |`-> (1): Sequential[9c084e64]{
        |      [input -> (1) -> (2) -> output]
        |      (1): SpatialConvolution[8e28ae20](192 -> 64, 1 x 1, 1, 1, 0, 0)
        |      (2): ReLU[125a7ae6](0.0, 0.0)
        |    }
        |`-> (2): Sequential[3417f636]{
        |      [input -> (1) -> (2) -> (3) -> (4) -> output]
        |      (1): SpatialConvolution[610540fc](192 -> 96, 1 x 1, 1, 1, 0, 0)
        |      (2): ReLU[78138285](0.0, 0.0)
        |      (3): SpatialConvolution[97de1222](96 -> 128, 3 x 3, 1, 1, 1, 1)
        |      (4): ReLU[ab2a7dc](0.0, 0.0)
        |    }
        |`-> (3): Sequential[7492b6e0]{
        |      [input -> (1) -> (2) -> (3) -> (4) -> output]
        |      (1): SpatialConvolution[ce424f90](192 -> 16, 1 x 1, 1, 1, 0, 0)
        |      (2): ReLU[958b0cc0](0.0, 0.0)
        |      (3): SpatialConvolution[308454db](16 -> 32, 5 x 5, 1, 1, 2, 2)
        |      (4): ReLU[51e50505](0.0, 0.0)
        |    }
        |`-> (4): Sequential[af088e82]{
               [input -> (1) -> (2) -> (3) -> output]
               (1): SpatialMaxPooling[66fe526a](3, 3, 1, 1, 1, 1)
               (2): SpatialConvolution[dddbe1a6](192 -> 32, 1 x 1, 1, 1, 0, 0)
               (3): ReLU[858128cc](0.0, 0.0)
             }
         ... -> output
      }
    (10): Concat[21722d07]{
      input
        |`-> (1): Sequential[f042b582]{
        |      [input -> (1) -> (2) -> output]
        |      (1): SpatialConvolution[f4159952](256 -> 128, 1 x 1, 1, 1, 0, 0)
        |      (2): ReLU[98d756f5](0.0, 0.0)
        |    }
        |`-> (2): Sequential[efbf0cdc]{
        |      [input -> (1) -> (2) -> (3) -> (4) -> output]
        |      (1): SpatialConvolution[2f5f779c](256 -> 128, 1 x 1, 1, 1, 0, 0)
        |      (2): ReLU[5dd4867d](0.0, 0.0)
        |      (3): SpatialConvolution[13fa7bd9](128 -> 192, 3 x 3, 1, 1, 1, 1)
        |      (4): ReLU[985a5f31](0.0, 0.0)
        |    }
        |`-> (3): Sequential[179c7d31]{
        |      [input -> (1) -> (2) -> (3) -> (4) -> output]
        |      (1): SpatialConvolution[c2edee0](256 -> 32, 1 x 1, 1, 1, 0, 0)
        |      (2): ReLU[809b7d5f](0.0, 0.0)
        |      (3): SpatialConvolution[dabbb85c](32 -> 96, 5 x 5, 1, 1, 2, 2)
        |      (4): ReLU[7e329879](0.0, 0.0)
        |    }
        |`-> (4): Sequential[9832530d]{
               [input -> (1) -> (2) -> (3) -> output]
               (1): SpatialMaxPooling[566a9109](3, 3, 1, 1, 1, 1)
               (2): SpatialConvolution[126cec0a](256 -> 64, 1 x 1, 1, 1, 0, 0)
               (3): ReLU[53af00a6](0.0, 0.0)
             }
         ... -> output
      }
    (11): SpatialAveragePooling[71858b56](3, 3, 2, 2, 0, 0)
    (12): Concat[a9ed004a]{
      input
        |`-> (1): Sequential[4ff12b2f]{
        |      [input -> (1) -> (2) -> output]
        |      (1): SpatialConvolution[2caffe8e](480 -> 192, 1 x 1, 1, 1, 0, 0)
        |      (2): ReLU[12f2cfef](0.0, 0.0)
        |    }
        |`-> (2): Sequential[fbbddc93]{
        |      [input -> (1) -> (2) -> (3) -> (4) -> output]
        |      (1): SpatialConvolution[4fd13e0e](480 -> 96, 1 x 1, 1, 1, 0, 0)
        |      (2): ReLU[bc3bbe44](0.0, 0.0)
        |      (3): SpatialConvolution[370c25d8](96 -> 208, 3 x 3, 1, 1, 1, 1)
        |      (4): ReLU[49db959f](0.0, 0.0)
        |    }
        |`-> (3): Sequential[c175be40]{
        |      [input -> (1) -> (2) -> (3) -> (4) -> output]
        |      (1): SpatialConvolution[5143053e](480 -> 16, 1 x 1, 1, 1, 0, 0)
        |      (2): ReLU[69fe1a64](0.0, 0.0)
        |      (3): SpatialConvolution[e66233ac](16 -> 48, 5 x 5, 1, 1, 2, 2)
        |      (4): ReLU[168c44c](0.0, 0.0)
        |    }
        |`-> (4): Sequential[fcc8fd24]{
               [input -> (1) -> (2) -> (3) -> output]
               (1): SpatialMaxPooling[64434d41](3, 3, 1, 1, 1, 1)
               (2): SpatialConvolution[acffea02](480 -> 64, 1 x 1, 1, 1, 0, 0)
               (3): ReLU[73e41a06](0.0, 0.0)
             }
         ... -> output
      }
  }
  (2): Concat[8dd61f67]{
    input
      |`-> (1): Sequential[3fc766a0]{
      |      [input -> (1) -> (2) -> output]
      |      (1): Sequential[efaa885c]{
      |        [input -> (1) -> (2) -> (3) -> output]
      |        (1): Concat[f9a4d342]{
      |          input
      |            |`-> (1): Sequential[a134136a]{
      |            |      [input -> (1) -> (2) -> output]
      |            |      (1): SpatialConvolution[8324ec8a](512 -> 160, 1 x 1, 1, 1, 0, 0)
      |            |      (2): ReLU[c40e58fd](0.0, 0.0)
      |            |    }
      |            |`-> (2): Sequential[c086fdb3]{
      |            |      [input -> (1) -> (2) -> (3) -> (4) -> output]
      |            |      (1): SpatialConvolution[78fb0b47](512 -> 112, 1 x 1, 1, 1, 0, 0)
      |            |      (2): ReLU[260be9d1](0.0, 0.0)
      |            |      (3): SpatialConvolution[c9569bbe](112 -> 224, 3 x 3, 1, 1, 1, 1)
      |            |      (4): ReLU[d80ddd3b](0.0, 0.0)
      |            |    }
      |            |`-> (3): Sequential[753931f2]{
      |            |      [input -> (1) -> (2) -> (3) -> (4) -> output]
      |            |      (1): SpatialConvolution[96ece8c7](512 -> 24, 1 x 1, 1, 1, 0, 0)
      |            |      (2): ReLU[bc1ab832](0.0, 0.0)
      |            |      (3): SpatialConvolution[d929c291](24 -> 64, 5 x 5, 1, 1, 2, 2)
      |            |      (4): ReLU[44be5a3a](0.0, 0.0)
      |            |    }
      |            |`-> (4): Sequential[496e19b1]{
      |                   [input -> (1) -> (2) -> (3) -> output]
      |                   (1): SpatialMaxPooling[c553c9a4](3, 3, 1, 1, 1, 1)
      |                   (2): SpatialConvolution[7c71e182](512 -> 64, 1 x 1, 1, 1, 0, 0)
      |                   (3): ReLU[54570fc9](0.0, 0.0)
      |                 }
      |             ... -> output
      |          }
      |        (2): Concat[90647082]{
      |          input
      |            |`-> (1): Sequential[db1db6a8]{
      |            |      [input -> (1) -> (2) -> output]
      |            |      (1): SpatialConvolution[915f8b02](512 -> 128, 1 x 1, 1, 1, 0, 0)
      |            |      (2): ReLU[d12ebb9f](0.0, 0.0)
      |            |    }
      |            |`-> (2): Sequential[98d70dcf]{
      |            |      [input -> (1) -> (2) -> (3) -> (4) -> output]
      |            |      (1): SpatialConvolution[672491cd](512 -> 128, 1 x 1, 1, 1, 0, 0)
      |            |      (2): ReLU[ef540399](0.0, 0.0)
      |            |      (3): SpatialConvolution[53a08f50](128 -> 256, 3 x 3, 1, 1, 1, 1)
      |            |      (4): ReLU[3f2c98dc](0.0, 0.0)
      |            |    }
      |            |`-> (3): Sequential[5b70ef38]{
      |            |      [input -> (1) -> (2) -> (3) -> (4) -> output]
      |            |      (1): SpatialConvolution[e6004581](512 -> 24, 1 x 1, 1, 1, 0, 0)
      |            |      (2): ReLU[6f28b16](0.0, 0.0)
      |            |      (3): SpatialConvolution[7b61c481](24 -> 64, 5 x 5, 1, 1, 2, 2)
      |            |      (4): ReLU[dff763f9](0.0, 0.0)
      |            |    }
      |            |`-> (4): Sequential[c8f24fa4]{
      |                   [input -> (1) -> (2) -> (3) -> output]
      |                   (1): SpatialMaxPooling[7a8e06ca](3, 3, 1, 1, 1, 1)
      |                   (2): SpatialConvolution[17e6c3e9](512 -> 64, 1 x 1, 1, 1, 0, 0)
      |                   (3): ReLU[19a9a5df](0.0, 0.0)
      |                 }
      |             ... -> output
      |          }
      |        (3): Concat[461d2955]{
      |          input
      |            |`-> (1): Sequential[cab5604]{
      |            |      [input -> (1) -> (2) -> output]
      |            |      (1): SpatialConvolution[ef9e33c3](152 -> 112, 1 x 1, 1, 1, 0, 0)
      |            |      (2): ReLU[7023e4b4](0.0, 0.0)
      |            |    }
      |            |`-> (2): Sequential[96d497fc]{
      |            |      [input -> (1) -> (2) -> (3) -> (4) -> output]
      |            |      (1): SpatialConvolution[619eba1f](152 -> 144, 1 x 1, 1, 1, 0, 0)
      |            |      (2): ReLU[fd4b9b](0.0, 0.0)
      |            |      (3): SpatialConvolution[4f8f11a4](144 -> 288, 3 x 3, 1, 1, 1, 1)
      |            |      (4): ReLU[c755aaa3](0.0, 0.0)
      |            |    }
      |            |`-> (3): Sequential[d3f88c88]{
      |            |      [input -> (1) -> (2) -> (3) -> (4) -> output]
      |            |      (1): SpatialConvolution[b5e560cf](152 -> 32, 1 x 1, 1, 1, 0, 0)
      |            |      (2): ReLU[51df5575](0.0, 0.0)
      |            |      (3): SpatialConvolution[81da8c0e](32 -> 64, 5 x 5, 1, 1, 2, 2)
      |            |      (4): ReLU[3d6b8f2](0.0, 0.0)
      |            |    }
      |            |`-> (4): Sequential[16ab4d02]{
      |                   [input -> (1) -> (2) -> (3) -> output]
      |                   (1): SpatialMaxPooling[344b0dc4](3, 3, 1, 1, 1, 1)
      |                   (2): SpatialConvolution[6fc910a7](152 -> 64, 1 x 1, 1, 1, 0, 0)
      |                   (3): ReLU[d97e5f9f](0.0, 0.0)
      |                 }
      |             ... -> output
      |          }
      |      }
      |      (2): Concat[e7758dee]{
      |        input
      |          |`-> (1): Sequential[94afa29a]{
      |          |      [input -> (1) -> (2) -> output]
      |          |      (1): Sequential[6f6cd0b1]{
      |          |        [input -> (1) -> (2) -> (3) -> (4) -> output]
      |          |        (1): Concat[533a0343]{
      |          |          input
      |          |            |`-> (1): Sequential[e309c3d8]{
      |          |            |      [input -> (1) -> (2) -> output]
      |          |            |      (1): SpatialConvolution[5946e252](528 -> 256, 1 x 1, 1, 1, 0, 0)
      |          |            |      (2): ReLU[7d44b0a2](0.0, 0.0)
      |          |            |    }
      |          |            |`-> (2): Sequential[d6b67797]{
      |          |            |      [input -> (1) -> (2) -> (3) -> (4) -> output]
      |          |            |      (1): SpatialConvolution[36555c05](528 -> 160, 1 x 1, 1, 1, 0, 0)
      |          |            |      (2): ReLU[15c63d53](0.0, 0.0)
      |          |            |      (3): SpatialConvolution[f5e1662a](160 -> 320, 3 x 3, 1, 1, 1, 1)
      |          |            |      (4): ReLU[939f4c92](0.0, 0.0)
      |          |            |    }
      |          |            |`-> (3): Sequential[412905ad]{
      |          |            |      [input -> (1) -> (2) -> (3) -> (4) -> output]
      |          |            |      (1): SpatialConvolution[c874f932](528 -> 32, 1 x 1, 1, 1, 0, 0)
      |          |            |      (2): ReLU[d86955fe](0.0, 0.0)
      |          |            |      (3): SpatialConvolution[cb59037c](32 -> 128, 5 x 5, 1, 1, 2, 2)
      |          |            |      (4): ReLU[4f24c58f](0.0, 0.0)
      |          |            |    }
      |          |            |`-> (4): Sequential[b3bd80c4]{
      |          |                   [input -> (1) -> (2) -> (3) -> output]
      |          |                   (1): SpatialMaxPooling[fab8737e](3, 3, 1, 1, 1, 1)
      |          |                   (2): SpatialConvolution[f027d557](528 -> 128, 1 x 1, 1, 1, 0, 0)
      |          |                   (3): ReLU[81274854](0.0, 0.0)
      |          |                 }
      |          |             ... -> output
      |          |          }
      |          |        (2): SpatialMaxPooling[5b0c762f](3, 3, 2, 2, 0, 0)
      |          |        (3): Concat[196d7b40]{
      |          |          input
      |          |            |`-> (1): Sequential[3a13ba0d]{
      |          |            |      [input -> (1) -> (2) -> output]
      |          |            |      (1): SpatialConvolution[8693953d](832 -> 256, 1 x 1, 1, 1, 0, 0)
      |          |            |      (2): ReLU[dcf6c89a](0.0, 0.0)
      |          |            |    }
      |          |            |`-> (2): Sequential[14792ef5]{
      |          |            |      [input -> (1) -> (2) -> (3) -> (4) -> output]
      |          |            |      (1): SpatialConvolution[76385bdc](832 -> 160, 1 x 1, 1, 1, 0, 0)
      |          |            |      (2): ReLU[4975bf27](0.0, 0.0)
      |          |            |      (3): SpatialConvolution[3d1d5a03](160 -> 320, 3 x 3, 1, 1, 1, 1)
      |          |            |      (4): ReLU[42d4be2b](0.0, 0.0)
      |          |            |    }
      |          |            |`-> (3): Sequential[24eb7edc]{
      |          |            |      [input -> (1) -> (2) -> (3) -> (4) -> output]
      |          |            |      (1): SpatialConvolution[520e1465](832 -> 32, 1 x 1, 1, 1, 0, 0)
      |          |            |      (2): ReLU[d9d146e5](0.0, 0.0)
      |          |            |      (3): SpatialConvolution[3c0187d0](32 -> 128, 5 x 5, 1, 1, 2, 2)
      |          |            |      (4): ReLU[55f2106](0.0, 0.0)
      |          |            |    }
      |          |            |`-> (4): Sequential[d2ea00df]{
      |          |                   [input -> (1) -> (2) -> (3) -> output]
      |          |                   (1): SpatialMaxPooling[cccff5b7](3, 3, 1, 1, 1, 1)
      |          |                   (2): SpatialConvolution[29ccbeeb](832 -> 128, 1 x 1, 1, 1, 0, 0)
      |          |                   (3): ReLU[88062f79](0.0, 0.0)
      |          |                 }
      |          |             ... -> output
      |          |          }
      |          |        (4): Concat[28caa4e1]{
      |          |          input
      |          |            |`-> (1): Sequential[d5d772cb]{
      |          |            |      [input -> (1) -> (2) -> output]
      |          |            |      (1): SpatialConvolution[36870e5d](832 -> 384, 1 x 1, 1, 1, 0, 0)
      |          |            |      (2): ReLU[b023523e](0.0, 0.0)
      |          |            |    }
      |          |            |`-> (2): Sequential[f48fa5c2]{
      |          |            |      [input -> (1) -> (2) -> (3) -> (4) -> output]
      |          |            |      (1): SpatialConvolution[cb8df4f7](832 -> 192, 1 x 1, 1, 1, 0, 0)
      |          |            |      (2): ReLU[fc1b1b51](0.0, 0.0)
      |          |            |      (3): SpatialConvolution[ffc8f13b](192 -> 384, 3 x 3, 1, 1, 1, 1)
      |          |            |      (4): ReLU[7873d78f](0.0, 0.0)
      |          |            |    }
      |          |            |`-> (3): Sequential[e98d48b7]{
      |          |            |      [input -> (1) -> (2) -> (3) -> (4) -> output]
      |          |            |      (1): SpatialConvolution[2d5b2f85](832 -> 48, 1 x 1, 1, 1, 0, 0)
      |          |            |      (2): ReLU[c1aa8bc9](0.0, 0.0)
      |          |            |      (3): SpatialConvolution[330747a7](48 -> 128, 5 x 5, 1, 1, 2, 2)
      |          |            |      (4): ReLU[d7df310a](0.0, 0.0)
      |          |            |    }
      |          |            |`-> (4): Sequential[847dac24]{
      |          |                   [input -> (1) -> (2) -> (3) -> output]
      |          |                   (1): SpatialMaxPooling[6fc196a](3, 3, 1, 1, 1, 1)
      |          |                   (2): SpatialConvolution[862e33e4](832 -> 128, 1 x 1, 1, 1, 0, 0)
      |          |                   (3): ReLU[5c0bd7d8](0.0, 0.0)
      |          |                 }
      |          |             ... -> output
      |          |          }
      |          |      }
      |          |      (2): Sequential[256014bf]{
      |          |        [input -> (1) -> (2) -> (3) -> (4) -> (5) -> (6) -> output]
      |          |        (1): SpatialAveragePooling[3010f829](7, 7, 1, 1, 0, 0)
      |          |        (2): View[b32409e](1024)
      |          |        (3): Dropout[a42cf92b](0.4)
      |          |        (4): Linear[613ee2d8](1024 -> 1000)
      |          |        (5): ReLU[36a22bfd](0.0, 0.0)
      |          |        (6): LogSoftMax[abb84030]
      |          |      }
      |          |    }
      |          |`-> (2): Sequential[9aab406d]{
      |                 [input -> (1) -> (2) -> (3) -> (4) -> (5) -> (6) -> (7) -> (8) -> (9) -> (10) -> output]
      |                 (1): SpatialAveragePooling[3494d0b8](5, 5, 3, 3, 0, 0)
      |                 (2): SpatialConvolution[2d492538](512 -> 128, 1 x 1, 1, 1, 0, 0)
      |                 (3): ReLU[7c2050f7](0.0, 0.0)
      |                 (4): View[e8413649](2048)
      |                 (5): Linear[4045692f](2048 -> 1024)
      |                 (6): ReLU[f78fa942](0.0, 0.0)
      |                 (7): Dropout[1036b886](0.7)
      |                 (8): Linear[b83eb2](1024 -> 1000)
      |                 (9): ReLU[c8ee20ce](0.0, 0.0)
      |                 (10): LogSoftMax[75386a5e]
      |               }
      |           ... -> output
      |        }
      |    }
      |`-> (2): Sequential[af4357ab]{
             [input -> (1) -> (2) -> (3) -> (4) -> (5) -> (6) -> (7) -> (8) -> (9) -> (10) -> output]
             (1): SpatialAveragePooling[a88c4a1f](5, 5, 3, 3, 0, 0)
             (2): SpatialConvolution[5de5fdf1](512 -> 128, 1 x 1, 1, 1, 0, 0)
             (3): ReLU[d454aede](0.0, 0.0)
             (4): View[f57d1272](2048)
             (5): Linear[5c9eeb56](2048 -> 1024)
             (6): ReLU[57e6ee2b](0.0, 0.0)
             (7): Dropout[7b20876b](0.7)
             (8): Linear[aed4c289](1024 -> 1000)
             (9): ReLU[b57815ff](0.0, 0.0)
             (10): LogSoftMax[249f23cb]
           }
       ... -> output
    }
}
 */

/** Object represents GoogleNetModel */
object GoogLeNetModel {
  
  /** creates an instance of GoogleNetModel model */
    def build(Height:Int,Width:Int,classNum: Int)={
      
      /////////////////////////////////////////////////////////////////////////////////////
      //Building the inception module
      def inc(inputSize: Int, config: Table, namePrefix : String = ""): Module[Float] ={
       val concat = Concat(2)
        val conv1 = Sequential()
        conv1.add(SpatialConvolution(inputSize,
          config[Table](1)(1), 1, 1, 1, 1)
          .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "1x1"))
        conv1.add(ReLU(true).setName(namePrefix + "relu_1x1"))
        concat.add(conv1)
        val conv3 = Sequential()
        conv3.add(SpatialConvolution(inputSize,
          config[Table](2)(1), 1, 1, 1, 1)
          .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "3x3_reduce"))
        conv3.add(ReLU(true).setName(namePrefix + "relu_3x3_reduce"))
        conv3.add(SpatialConvolution(config[Table](2)(1),
          config[Table](2)(2), 3, 3, 1, 1, 1, 1)
          .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "3x3"))
        conv3.add(ReLU(true).setName(namePrefix + "relu_3x3"))
        concat.add(conv3)
        val conv5 = Sequential()
        conv5.add(SpatialConvolution(inputSize,
          config[Table](3)(1), 1, 1, 1, 1)
          .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "5x5_reduce"))
        conv5.add(ReLU(true).setName(namePrefix + "relu_5x5_reduce"))
        conv5.add(SpatialConvolution(config[Table](3)(1),
          config[Table](3)(2), 5, 5, 1, 1, 2, 2)
          .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "5x5"))
        conv5.add(ReLU(true).setName(namePrefix + "relu_5x5"))
        concat.add(conv5)
        val pool = Sequential()
        pool.add(SpatialMaxPooling(3, 3, 1, 1, 1, 1).ceil().setName(namePrefix + "pool"))
        pool.add(SpatialConvolution(inputSize,
          config[Table](4)(1), 1, 1, 1, 1)
          .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "pool_proj"))
        pool.add(ReLU(true).setName(namePrefix + "relu_pool_proj"))
        concat.add(pool).setName(namePrefix + "output")
    concat
    }
    /////////////////////////////////////////////////////////////////////////////////////
      
      
    /////////////////////////////////////////////////////////////////////////////////////
    //first layer factorize convolution
    def fac()={
      val conv=Sequential()
      conv.add(Contiguous())
      //View the input as three of one plane
      conv.add(View(-1,1,224,224))
      conv.add(SpatialConvolution(1,8,7,7,2,2,3,3))
      
      val depthWiseConv=ParallelTable()
      depthWiseConv.add(conv) //R
      depthWiseConv.add(conv.cloneModule()) //G
      depthWiseConv.add(conv.cloneModule()) //B
      
      val factorised=Sequential()
      factorised.add(depthWiseConv)
      factorised.add(ReLU(true))
      factorised.add(SpatialConvolution(24,64,1,1))
      factorised.add(ReLU(true))
      factorised
    }
    /////////////////////////////////////////////////////////////////////////////////////
    
    
      //Building the blocks
      val feature1 = Sequential()
      feature1.add(SpatialZeroPadding(0, 224-Width, 0, 224-Height))
      feature1.add(Padding(1,-2,3,value=0.0,nIndex=1))
      feature1.add(SpatialConvolution(3, 64, 7, 7, 2, 2, 3, 3, 1, false)
        .setInitMethod(weightInitMethod = Xavier, Zeros)
        .setName("conv1/7x7_s2"))
      feature1.add(ReLU(true).setName("conv1/relu_7x7"))
      feature1.add(SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool1/3x3_s2"))
      feature1.add(SpatialCrossMapLRN(5, 0.0001, 0.75).setName("pool1/norm1"))
      feature1.add(SpatialConvolution(64, 64, 1, 1, 1, 1)
        .setInitMethod(weightInitMethod = Xavier, Zeros)
        .setName("conv2/3x3_reduce"))
      feature1.add(ReLU(true).setName("conv2/relu_3x3_reduce"))
      feature1.add(SpatialConvolution(64, 192, 3, 3, 1, 1, 1, 1)
        .setInitMethod(weightInitMethod = Xavier, Zeros)
        .setName("conv2/3x3"))
      feature1.add(ReLU(true).setName("conv2/relu_3x3"))
      feature1.add(SpatialCrossMapLRN(5, 0.0001, 0.75). setName("conv2/norm2"))
      feature1.add(SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool2/3x3_s2"))
      feature1.add(inc(192, T(T(64), T(96, 128), T(16, 32), T(32)), "inception_3a/"))
      feature1.add(inc(256, T(T(128), T(128, 192), T(32, 96), T(64)), "inception_3b/"))
      feature1.add(SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool3/3x3_s2"))
      feature1.add(inc(480, T(T(192), T(96, 208), T(16, 48), T(64)), "inception_4a/"))
  
      val output1 = Sequential()
      output1.add(SpatialAveragePooling(5, 5, 3, 3).ceil().setName("loss1/ave_pool"))
      output1.add(SpatialConvolution(512, 128, 1, 1, 1, 1).setName("loss1/conv"))
      output1.add(ReLU(true).setName("loss1/relu_conv"))
      output1.add(View(128 * 4 * 4).setNumInputDims(3))
      output1.add(Linear(128 * 4 * 4, 1024).setName("loss1/fc"))
      output1.add(ReLU(true).setName("loss1/relu_fc"))
      output1.add(Dropout(0.7).setName("loss1/drop_fc"))
      output1.add(Linear(1024, classNum).setName("loss1/classifier"))
      output1.add(LogSoftMax().setName("loss1/loss"))
  
      val feature2 = Sequential()
      feature2.add(inc(512, T(T(160), T(112, 224), T(24, 64), T(64)), "inception_4b/"))
      feature2.add(inc(512, T(T(128), T(128, 256), T(24, 64), T(64)), "inception_4c/"))
      feature2.add(inc(512, T(T(112), T(144, 288), T(32, 64), T(64)), "inception_4d/"))
  
      val output2 = Sequential()
      output2.add(SpatialAveragePooling(5, 5, 3, 3).setName("loss2/ave_pool"))
      output2.add(SpatialConvolution(528, 128, 1, 1, 1, 1).setName("loss2/conv"))
      output2.add(ReLU(true).setName("loss2/relu_conv"))
      output2.add(View(128 * 4 * 4).setNumInputDims(3))
      output2.add(Linear(128 * 4 * 4, 1024).setName("loss2/fc"))
      output2.add(ReLU(true).setName("loss2/relu_fc"))
      output2.add(Dropout(0.7).setName("loss2/drop_fc"))
      output2.add(Linear(1024, classNum).setName("loss2/classifier"))
      output2.add(LogSoftMax().setName("loss2/loss"))
  
      val output3 = Sequential()
      output3.add(inc(528, T(T(256), T(160, 320), T(32, 128), T(128)),
        "inception_4e/"))
      output3.add(SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool4/3x3_s2"))
      output3.add(inc(832, T(T(256), T(160, 320), T(32, 128), T(128)),
        "inception_5a/"))
      output3.add(inc(832, T(T(384), T(192, 384), T(48, 128), T(128)),
        "inception_5b/"))
      output3.add(SpatialAveragePooling(7, 7, 1, 1).setName("pool5/7x7_s1"))
      output3.add(Dropout(0.4).setName("pool5/drop_7x7_s1"))
      output3.add(View(1024).setNumInputDims(3))
      output3.add(Linear(1024, classNum)
        .setInitMethod(weightInitMethod = Xavier, Zeros).setName("loss3/classifier"))
      output3.add(LogSoftMax().setName("loss3/loss3"))
  
      val split2 = Concat(2).setName("split2")
      split2.add(output3)
      split2.add(output2)
  
      val mainBranch = Sequential()
      mainBranch.add(feature2)
      mainBranch.add(split2)
  
      val split1 = Concat(2).setName("split1")
      split1.add(mainBranch)
      split1.add(output1)
  
      val model = Sequential()
  
      model.add(feature1)
      model.add(split1)
  
  model
  }
    
    
    def inc_g(input: ModuleNode[Float], inputSize: Int, config: Table, namePrefix : String): ModuleNode[Float] = {
   
    val conv1x1 = SpatialConvolution(inputSize, config[Table](1)(1), 1, 1, 1, 1)
        .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "1x1").inputs(input)
    val relu1x1 = ReLU(true).setName(namePrefix + "relu_1x1").inputs(conv1x1)

    val conv3x3_1 = SpatialConvolution(inputSize, config[Table](2)(1), 1, 1, 1, 1).setInitMethod(
      weightInitMethod = Xavier, Zeros).setName(namePrefix + "3x3_reduce").inputs(input)
    val relu3x3_1 = ReLU(true).setName(namePrefix + "relu_3x3_reduce").inputs(conv3x3_1)
    val conv3x3_2 = SpatialConvolution(
      config[Table](2)(1), config[Table](2)(2), 3, 3, 1, 1, 1, 1)
      .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "3x3").inputs(relu3x3_1)
    val relu3x3_2 = ReLU(true).setName(namePrefix + "relu_3x3").inputs(conv3x3_2)

    val conv5x5_1 = SpatialConvolution(inputSize, config[Table](3)(1), 1, 1, 1, 1).setInitMethod(
      weightInitMethod = Xavier, Zeros).setName(namePrefix + "5x5_reduce").inputs(input)
    val relu5x5_1 = ReLU(true).setName(namePrefix + "relu_5x5_reduce").inputs(conv5x5_1)
    val conv5x5_2 = SpatialConvolution(
      config[Table](3)(1), config[Table](3)(2), 5, 5, 1, 1, 2, 2)
      .setInitMethod(weightInitMethod = Xavier, Zeros).setName(namePrefix + "5x5").inputs(relu5x5_1)
    val relu5x5_2 = ReLU(true).setName(namePrefix + "relu_5x5").inputs(conv5x5_2)

    val pool = SpatialMaxPooling(3, 3, 1, 1, 1, 1).ceil()
      .setName(namePrefix + "pool").inputs(input)
    val convPool = SpatialConvolution(inputSize, config[Table](4)(1), 1, 1, 1, 1).setInitMethod(
      weightInitMethod = Xavier, Zeros).setName(namePrefix + "pool_proj").inputs(pool)
    val reluPool = ReLU(true).setName(namePrefix + "relu_pool_proj").inputs(convPool)

    JoinTable(2, 0).inputs(relu1x1, relu3x3_2, relu5x5_2, reluPool)
    }
    
    
    
    
    def graph(Height:Int,Width:Int,classNum: Int)={
    val input = Input()
    val conv1 = SpatialConvolution(3, 64, 7, 7, 2, 2, 3, 3, 1, false)
      .setInitMethod(weightInitMethod = Xavier, Zeros)
      .setName("conv1/7x7_s2").inputs(input)
    val relu1 = ReLU(true).setName("conv1/relu_7x7").inputs(conv1)
    val pool1 = SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool1/3x3_s2").inputs(relu1)
    val lrn1 = SpatialCrossMapLRN(5, 0.0001, 0.75).setName("pool1/norm1").inputs(pool1)
    val conv2 = SpatialConvolution(64, 64, 1, 1, 1, 1)
      .setInitMethod(weightInitMethod = Xavier, Zeros)
      .setName("conv2/3x3_reduce").inputs(lrn1)
    val relu2 = ReLU(true).setName("conv2/relu_3x3_reduce").inputs(conv2)
    val conv3 = SpatialConvolution(64, 192, 3, 3, 1, 1, 1, 1)
      .setInitMethod(weightInitMethod = Xavier, Zeros)
      .setName("conv2/3x3").inputs(relu2)
    val relu3 = ReLU(true).setName("conv2/relu_3x3").inputs(conv3)
    val lrn2 = SpatialCrossMapLRN(5, 0.0001, 0.75). setName("conv2/norm2").inputs(relu3)
    val pool2 = SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool2/3x3_s2").inputs(lrn2)
    val layer1 = inc_g(pool2, 192, T(T(64), T(96, 128), T(16, 32), T(32)),
      "inception_3a/")
    val layer2 = inc_g(layer1, 256, T(T(128), T(128, 192), T(32, 96), T(64)),
      "inception_3b/")
    val pool3 = SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool3/3x3_s2").inputs(layer2)
    val feature1 = inc_g(pool3, 480, T(T(192), T(96, 208), T(16, 48), T(64)),
      "inception_4a/")

    val pool2_1 = SpatialAveragePooling(5, 5, 3, 3).ceil()
      .setName("loss1/ave_pool").inputs(feature1)
    val loss2_1 = SpatialConvolution(512, 128, 1, 1, 1, 1).setName("loss1/conv").inputs(pool2_1)
    val relu2_1 = ReLU(true).setName("loss1/relu_conv").inputs(loss2_1)
    val view2_1 = View(128 * 4 * 4).setNumInputDims(3).inputs(relu2_1)
    val linear2_1 = Linear(128 * 4 * 4, 1024).setName("loss1/fc").inputs(view2_1)
    val relu2_2 = ReLU(true).setName("loss1/relu_fc").inputs(linear2_1)
    val drop2_1 = Dropout(0.7).setName("loss1/drop_fc").inputs(relu2_2) 
    val classifier2_1 = Linear(1024, classNum).setName("loss1/classifier").inputs(drop2_1)
    val output1 = LogSoftMax().setName("loss1/loss").inputs(classifier2_1)

    val layer3_1 = inc_g(feature1, 512, T(T(160), T(112, 224), T(24, 64), T(64)),
      "inception_4b/")
    val layer3_2 = inc_g(layer3_1, 512, T(T(128), T(128, 256), T(24, 64), T(64)),
      "inception_4c/")
    val feature2 = inc_g(layer3_2, 512, T(T(112), T(144, 288), T(32, 64), T(64)),
      "inception_4d/")

    val pool4_1 = SpatialAveragePooling(5, 5, 3, 3).setName("loss2/ave_pool").inputs(feature2)
    val conv4_1 = SpatialConvolution(528, 128, 1, 1, 1, 1).setName("loss2/conv").inputs(pool4_1)
    val relu4_1 = ReLU(true).setName("loss2/relu_conv").inputs(conv4_1)
    val view4_1 = View(128 * 4 * 4).setNumInputDims(3).inputs(relu4_1)
    val linear4_1 = Linear(128 * 4 * 4, 1024).setName("loss2/fc").inputs(view4_1)
    val relu4_2 = ReLU(true).setName("loss2/relu_fc").inputs(linear4_1)
    val drop4_1 = Dropout(0.7).setName("loss2/drop_fc").inputs(relu4_2)
    val linear4_2 = Linear(1024, classNum).setName("loss2/classifier").inputs(drop4_1)
    val output2 = LogSoftMax().setName("loss2/loss").inputs(linear4_2)

    val layer5_1 = inc_g(feature2, 528, T(T(256), T(160, 320), T(32, 128), T(128)),
      "inception_4e/")
    val pool5_1 = SpatialMaxPooling(3, 3, 2, 2).ceil().setName("pool4/3x3_s2").inputs(layer5_1)
    val layer5_2 = inc_g(pool5_1, 832, T(T(256), T(160, 320), T(32, 128), T(128)),
      "inception_5a/")
    val layer5_3 = inc_g(layer5_2, 832, T(T(384), T(192, 384), T(48, 128), T(128)),
      "inception_5b/")
    val pool5_4 = SpatialAveragePooling(7, 7, 1, 1).setName("pool5/7x7_s1").inputs(layer5_3)
    val drop5_1 = Dropout(0.4).setName("pool5/drop_7x7_s1")
      .inputs(pool5_4)
    val view5_1 = View(1024).setNumInputDims(3).inputs(drop5_1)
    val linear5_1 = Linear(1024, classNum)
      .setInitMethod(weightInitMethod = Xavier, Zeros).setName("loss3/classifier").inputs(view5_1)
    val output3 = LogSoftMax().setName("loss3/loss3").inputs(linear5_1)

    val split2 = JoinTable(2, 0).setName("split2").inputs(output3, output2, output1)
    Graph(input, split2)
    }
    /*
     *	     +-------+      +-------+        +-------+
  					 | main0 +--+---> main1 +----+---> main2 +----+
  					 +-------+ |   +-------+    |   +-------+    |
            	   	     |                |                |
        	    	       | +----------+   | +----------+   | +----------+
        		  	      +-> softMax0 +-+  +-> softMax1 +-+ +-> softMax2 +-+
        		             +----------+ |   +----------+ |   +----------+ |
            		                      |                |                |   +-------+
            		                      +----------------v----------------v--->  out  |
             			                                                          +-------+
     */
    
    
}