/*
 * This file is licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.tresamigos.smv

class AsmUtilTest extends SmvUnitSpec {
  object ModuleWithAnonFun {
    val fib: Int => Int = { n =>
      @annotation.tailrec
      def iter (n1: Int, n2: Int, count: Int): Int = count match {
        case 1 => n1
        case 2 => n2
        case _ => iter(n2, n1 + n2, count - 1)
      }
      iter(1, 1, n)
    }
  }

  "AsmUtil.hasAnonfun" should  "identify modules with anonymous functions" in {
    AsmUtil.hasAnonfun(ModuleWithAnonFun.getClass.getName, getClass.getClassLoader) shouldBe true
  }

  object EmptyModule
  object ModuleWithoutAnonFun {
    val `not$anonfun` = 1
    val closeenough = "not $anonfun'$ either"
  }
  it should "not identify modules with no anonymous functions" in {
    Seq(EmptyModule, ModuleWithoutAnonFun) foreach { m =>
      AsmUtil.hasAnonfun(m.getClass.getName, getClass.getClassLoader) shouldBe false
    }
  }
}
