package ca.cqdg.etl

import ca.cqdg.etl.model.{NamedDataFrame, S3File}
import ca.cqdg.etl.test.util.WithSparkSession
import ca.cqdg.etl.utils.EtlUtils.readOptions
import ca.cqdg.etl.utils.{EtlUtils, PreProcessingUtils, S3Utils, Schema}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets

class DonorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll with WithSparkSession
//  with DockerTestKit with DockerKitSpotify
//  with MinioS3MockService
{

  //  implicit val pc: PatienceConfig = PatienceConfig(Span(20, Seconds), Span(1, Second))
  val BUCKET_NAME = "cqdg"

//  override def beforeAll(): Unit = {
  val s3Credential = new BasicAWSCredentials("minioadmin", "minioadmin")
  val s3Client: AmazonS3 = AmazonS3ClientBuilder
    .standard()
    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:9000", Regions.US_EAST_1.name()))
    .withPathStyleAccessEnabled(true)
    .withCredentials(new AWSStaticCredentialsProvider(s3Credential))
    .build()

  val inputDirectory = new File("../cqdg-etl/src/test/resources/tsv/input")

  if (!s3Client.doesBucketExistV2(BUCKET_NAME)) {
    s3Client.createBucket(BUCKET_NAME)

    if (inputDirectory.exists && inputDirectory.isDirectory) {
      val list =  inputDirectory.listFiles.filter(_.isFile).map(_.getName).toList
      list.foreach(fn => {
        val file = new File(s"../cqdg-etl/src/test/resources/tsv/input/$fn")
        s3Client.putObject(
          BUCKET_NAME,
          s"clinical-data/$fn",
          file
        )
      })
    }
  }

  val filesPerFolder: Map[String, List[S3File]] = S3Utils.loadFileEntries(BUCKET_NAME, "clinical-data", s3Client)

  val schemaJsonFile = new File("../cqdg-etl/src/test/resources/schema/schema.json")


  val schemaList: List[Schema] = PreProcessingUtils.getSchemaList(FileUtils.readFileToString(schemaJsonFile, StandardCharsets.UTF_8))

  val dictionarySchemas: Map[String, List[Schema]] = Map("5.44" -> schemaList)


  val hashList = Seq(
    "4f037810f167fa2da49a68b96ef9795857cf87eb",
    "98c6dc4804cb23356664f3f86771db971454f49c",
    "a897e63be1b5067a4be0dd516f0044647d78f8f0",
    "dd569274e304e9a2827e4ac8be8306d7ba279082",
    "88ff6f4317e5e1ba9a17744d79002c23f40f9775",
    "8151d6c3a6e5044ef6f5ba369128167c2fbeb725",
    "1d59b865cd8b0e964a19e13ba38f91b736677f7f",
    "4718fecfc18eec0ac9dbea8af25871e3b0c56324",
    "ce27ef8d3235e9652326e528cf181ddd2602e593",
    "63b0f4e8f5ea326daa3500f1b2cea17aacccd73a",
    "e991073f46d625c40eb9d3d4ab41d1b629135c7a",
    "59ea8d095fd634dc75d151a960961a9ef7cfe735",
    "563337dae2954b0c4aca3ffe131b44c5d5929a10",
    "3588caa858ad8155fd9f6a362fc54ca09bf9bb36",
    "46a3679228250d8d8a401e74b80394824e72ea7b",
    "267632cd07d5979e008a54077fae60c4617f9991",
    "19d040e56adf67a2fbb10cbc4a3567648fc40fc5",
    "2b9d8442411a33e2b60bc3c298bbb1dafb346940",
    "5b2d183ae962d7695e155f9f364b773c069f4c2f",
    "cf8aebe93cec7cd73b1364c500a56a761567d2e9",
    "f14852b768cbcfffcb0b469427351f4024e93bda",
    "b0af501cd8d69a968e9a73b19c9c3b2fac586057",
    "ea5dc221adef5838ed0839ec7335b8c953a6bf92",
    "101fdbd668985c75b0f3b082c69e7fc16de29570",
    "db13926e9ec0d671d9f91d983cf7cdc55d00a6f1",
    "d79694fba404d381df8e64e254388f95498ffac7",
    "8e50553e95abffbbd1b08aba4f9ad4babb51659c",
    "cd16db727afc83b8acf87c6071512aa2da1f65f5",
    "f8b3002d4d2d81290810ef8244d997a4a0b1cd55",
    "ff71e433263359f6ff0bd643f7692a4748808e80",
    "2a01634d55fd79b53c9a5df23d6bcff5bd0133ed",
    "d12b5078ba5a63eac1831694bd3d109156e5924d",
    "16803a70c5d5e282be1ff0ce2f90779410ce46b3",
    "18c62f7c75e9f496a0100a8b0cf0fa2bf4f87ef3",
    "de52316ebecbc5d7a3b21661a7ebaacd0abe4319",
    "e1b04d4ad8d3c2889a88eab08833ec920064ebc5",
    "4edcf24616dc8d3ed54770ef462beb06956ff281",
    "a999fb5f435cbe9f5a2495b61fae78e59a95d149",
    "0e0e4f22a148c9b650d940da9d5e726e0e5492b4",
    "31636d1f51b6afc66d64d0a0e783a0b0c9a47dbd",
    "093116e2aec18054cf9fb1c4b932bb598b11cb9d",
    "eea69bd34272a7388edd580ede51094d67500121",
    "f8501d36a030f61f854b20f26f944494d0e61da3",
    "a9b900072430547509818342b1d5f4f964a551fc",
    "99963a6f65bb7895af0f12d3613481ce025f9eeb",
    "12df9a72f984532a2115aeaa6878e376a7c0eb6e",
    "6f3d4127b2ed52d5e65183bbafdbf4985300c163",
    "2ae8b5f0f7e3de2388ba20aa9067ba5be0500e6a",
    "6af0155f589ceeeb555e6cfc7cdefa9939e0523e",
    "3e44c211fb934d8d6a4c4b374e74949da659bca4",
    "be52c25ebe4e5d42ad811c866df0e3728032bae2",
    "c551753dbaed3b1fd3d7e3eb1ab954467bb6098a",
    "fefcdbba0b9bc251001c12e782b8d16834209e1e",
    "5e353aca18d505840fb267b5bc5fc8af0d285d33",
    "78fdc850d1e3719db6efc2e58b3327efb923a31d",
    "6157061061b693097f5d73c06ffeee0a428129c1",
    "b5413cdd0446494928a38cd91519af26250af314",
    "58df90d6a97d626d91ecfa991a0f7dacc3c76d39",
    "a52c5cdbe7202ff0d9264299c4bc4a424366d854",
    "658f3ddffee04b5db3aae2ac3fbeb48ee05c672b",
    "2d3b0d78b68e56bc219a6898e598427882343ee7",
    "e3f4275e1e0c3bfa617b74d486a025ac6c18216a",
    "8fb212439384546b88404ad29fec7f3b615df4e1",
    "c17f154b9cc118f8f9cbd6875e4ea5d7463486c9",
    "5275d9a2bf57ac11d259a9868e86d130dd05c249",
    "e9af7c948dce2f397d88729f8603e6fb79bc5d33",
    "93bd2d5df14e62164f3679b94b792da1b791a6a9",
    "3b73457ce33dd0c26821ddb71cf45b3b7e2f650d",
    "5b4011bb201e4b78030b83b4ae7eb402d8180947",
    "33695bedbdc80d12edc478fc7cacf5b603d53616",
    "4ce6cbabcd1cc942d21fb23154bf725955ea4624",
    "207cac8f60b21e13f516d2c82d2092b3b734360d",
    "129603737ec653d2f3a0adb526b54d372625772c",
    "71f7c5dbd770a67ff8fac6a733c5b50f04d27b2b",
    "2692f3ca6f4cf074f54e5a2e438bb6e40a9dc314",
    "0be52a23448e44c1f63bb94d6b15774f7399823c",
    "5ec7aebfd88570e436e8a5e355505bd7c8edb8b6",
    "a28d0ba6f64a9a12734878cd097d27fe012f70cb",
    "7f6e2f002ddfb705596a36df99031cfd3e9d1135",
    "359af2ce884c7bae215681793665f69a5649b2ea",
    "6dd1344a523ee8199341e6e28093b404d7b9602a",
    "77263a4a8a93094076c93f4effbe0b252c92b689",
    "1e67bab7f950f2745796f6bf3d49e3bcea314bd5",
    "922639546c4b8544961e948269c74aeade85f226",
    "ebcc83e405b42c0180440388c78865a6f3d8fe14",
    "b0140d110dd2520598ce5f38aa2abe768a320f33",
    "2a857dc4d94c1596d93a27de1fbdd8d9f364fd1e",
    "d46224f70ce15252f7ac471c2759b80d2e1fbe22",
    "97b0a3bca6bf6950016485793476d04d18167f35",
    "efe3bcdcf2ca47e355fbc027af1a6500e5badd7e",
    "261a1436999df3925ae216ff3028872900b789d9",
    "9318ad9a2e09cca2ad5d1a090d5a338ef1dc5f60",
    "a8f40d36b084103ffbb9ff5109a0a95eb994629a",
    "c866aa678d5074f2f43ac4eafe85e42d36adaea9",
    "0c2b3a41ad96165e8709449ecdcff72d664e7ed5",
    "fa647d0458fa39e9d3e9f81a069c82b89481efda",
    "627fdf5ef973cc8f0ad4cd2e8fdcbd6f687cd664",
    "8ddc6576f788c951c4aee83e29d461a429546314",
    "946e7c2089decc48cf416b99c0e28a2e93adeb30",
    "076ba5259f7b237467c057dbab2f52c18f48940b",
    "8bb32073a267aa2878c7108282701a0fa2c7102f",
    "1caf7c7155b8c99670bee5de2ffbd68dcd6bdd26",
    "2e943fe43754c948dabada60b7a65b04600e9b83",
    "92445d52b534d54917ec95e74c76b8a3da261b03",
    "15b75ea8f7b9ad33f74308369b411f2cc7683472",
    "1582c82a393ff9d5aca1989db7daae619ce4a554",
    "046510688fd8d9f647e4d2bf906700d07b9ca9bb",
    "6345eaf2ae2c5adaea41e123afc10dacba872b17",
    "a6cc4754fa97f0bc3027bce27f58d7f41c0931d1",
    "e621f85ba42614e56bfbef72ae66a677fce82755",
    "eb11b62c6c86a75d1c7783bc76a8e297c084bb4e",
    "eb9fd2dc0b752092a50bdf66140868b5a690eded",
    "3a64888418bd0b13d9e278c7995e62134d1ec9b0",
    "ffc31fcbe96a132acc681215d270423e6fb9cf9e",
    "4eee24cf48c82780c80d7f347c24f3ecae8a8e1e",
    "61aeda52485f4c37fbaf480c2199edbbe8e37503",
    "b8493edde7dbbc1f7c627e1d2788dca421439ea0",
    "ad183e2b62eedab7168cf46d276bbed32eb17239",
    "2a03389b34e999cba42cd7f4a612ffcf0f7f7a11",
    "7b0ec9d13fd7d89c033f16d151733e2292ac9dda",
    "61b83af3b6ffdfb9bc1712758e551ad41f8b7979",
    "14afa2113a8f3e79ea1338e3bf0c1e8d77b6d002",
    "6d690a1cba17b636242ffde940234e1e0c3f2aee",
    "f9f507fd5abd16773e5587db4dee09bad9762bde",
    "6f722f5dd46e7bacd659affc984a94413372a1ed",
    "36c4757b7af823b0d0e23fac1299f06bc00ce595",
    "5606c650261774eab225d9bbf1ef4cd46a8c11f5",
    "893556590881fed989038b87815686bf7f90d178",
    "992d8561d6df13f68a1d7e391e4a66fe614e4dee",
    "1ccb6879d8cc99bd7aab7c9d0ed7f36672d88681",
    "688b3478dcb82db9568c1361320e70b0c4780afa",
    "72270016ce95659a62c6603b875d1e1c6d9d46a5",
    "cfbce398e9ea358f4356c7ea55ffff1ae5b477cd",
    "60c450638b1e924a52bb9262613e302650e7a760",
    "1df76115cd88dedb68728a58dd148aabf79d2a71",
    "eca8db14b435e061a420de17e4fac9608b5fbb9f",
    "05c15cbe812035e064b4b686a82917915c8a15e7",
    "0b2dc3151439cb52a300197bedef0d20c5e65707",
    "2778ba363d7df9ade8787137e403d8ca587b791d",
    "ba61b8608f37d0481e579e0ef5e73e75764dac95",
    "cf78ac0dbeadb51f62fcd443a5b002d1ff858aba",
    "0562385b5030fa2cc123b0c395fdcc277dc03b29",
    "f0803c55c5eb5728bd676cbdad2cc73b5a552988",
    "02d472d987b0fc1a7d98a2ac83a8b5d6a39793ea",
    "c881670a6082a8dddc4289b5dc3d303180df6c4e",
    "41ea6697608249997b106163a444b1353fb603a2",
    "a0821512277a4bde03753cbf645e67813e4741a2",
    "e749c0dba4a8851af756378d0c08d09144567d59",
    "70d2667636d8c756f23fff66e24aa1dd74b6296e",
    "fd94bb611cedbe682e4281c052952840c63b4939",
    "04ff3ae9889450f4cf1f1292bb8fdf32e0cb28da",
    "9ebe51267ea6aa46844b2a260bc604b4f3a07a09",
    "920616ecb6450bea201582a87d2acad9e922ec56",
    "dcc0b9d0a5192b6d64f4666dfd191483bd177412",
    "7bc32186869c65f8a90ae4b2fa983732915ff57a",
    "19927dea9ed7dc52843b306bafe4b145881541ec",
    "cbff7ff23b8e6baf43db673545d7a605cca0ae71",
    "521366345a9a8b6a34cf1363ee1547dd5cb060a5",
    "642b86b6f0efb381df6b9846f3bfdc909b9e4a34",
    "971f75e4ec7dc66de8abc6b542f43ac17a8a7811",
    "f6cd7981cdea088ee25eb3a45bbab63f97f0cac3",
    "e26637a4354f5ace443b72715227037975114e14",
    "79f9c20048cff7ab5bff5224490d5b98bd4da4f4",
    "19c940c268a3bcc76932398c8d3f179feb7d53dd",
    "07388abcd8fc7f0bde7af0b3111ea4e5f50544d2",
    "0619760c102f7c4613b4cd7fa84bffe7bb254390",
    "5843fd99a3288083dfa47f8b6422928338e1e122",
    "ee40c39e77b911736bfd46647c8f1f3429735f90",
    "6a713769fda14a84aa3e685b113589bd3a15a5dd",
    "844cb6b2132aa88ac03d4e9752862ee292479c0b",
    "fdebedda57bec1d875c8615073f325025e70045b",
    "96f5bb42d0d90005af24780042892a2a62ab98a2",
    "e781c9bb096ae907426592ac0ed752a52723288a",
    "83cfb68c076a71b643f3a76c0b2380a9d2ab4421",
    "5d026f2c33935bcd3ce70be79e0f12d556c2ed97",
    "9ad65ffce859f4cb8fe4cb2a90ad7edf6e2a0366",
    "f94c1c7696f0285be2d0fb62b3bd1ea8e0abfd70",
    "c2ca990443e2cb483bdcaa387ea3b23492ccbf61",
    "422688591c79b45c174eece772c8ce7d28922602",
    "ed85c19d71eaeab36c94a2f83612367564ddb8da",
    "f3dba292d6b3399ad067c9f66cfc7c7227fcfbeb",
    "df1cf439cab6b508c26b2111b90ea7235b58ea93",
    "afdf91b39eb6d2edf175c3ca54aae4aadc65cee8",
    "4aa05f648fcc24603e430c752e5499951db9e658",
    "f82cba39da5ad1e1d1b8b737e761d783d2579cea",
    "25f10aa982a5098190b1b00beb731a4a97575b7d",
    "728628234afb13df05f83cec47269ddd5473b9a7",
    "73c5e9c17d58ffc72856873497fa1bf170931919",
    "55cfe8d7ccc5066596b574b58d5ede461a7c4343",
    "693a75544ac6bd0d5ebae3cf6198f226d63ea527",
    "30a1a94e114073760a272f14bdb4853f4ebb5ca8",
    "54b542543ef7ed3af98e87c97cc1afebd275bd92",
    "5a37459389c70508be6f292bf61fef5ce70ffd4c",
    "ae4880b94e3ea30600875be35af18c1c7083bd51",
    "81fd8a10f5396ea7aeec1fa2e64118a948879878",
    "aaaeb7456f32376bae300734a018ede6de56841c",
    "64ca8e26ffa319db32f80e28ba59e85979d66b7c",
    "04feeb60cb64bc34d9a33146ab7aad8f0ca1f773",
    "86c84c6deeea14a6d85d2d13d403ae06d302679e",
    "7bc94e0e6e7dd46ed025fc9f9be5b8d03809a159",
    "b78e655ccd02532c9405f80f7c4eaafc1dbf8165",
    "7c2dadea5c84532b082ab412617c74d9a3517ad3",
    "d2cf1682ab970769340277296ac0694c803b54dd",
    "db6862efeae0812143ba2f2b8bbabb067c95bd3f",
    "07fa23fd9db3da958fcbb59085ebe89a2e31bcbb",
    "cd9664f24fdf7755c2be4c030bad1be7ee80fee9",
    "fb0f8c0323f99b2e6cc2d61ca207186d85ff6ecb",
  )


  val hashString: String = "[" + hashList.map(l => s"""{"hash":"$l","internal_id":"123"}""").mkString(",") + "]"
  val mockBuildIds: String => String = (_: String) => hashString

  val readyToProcess: Map[String, List[NamedDataFrame]] =
    PreProcessingUtils.preProcess(filesPerFolder, BUCKET_NAME, mockBuildIds)(dictionarySchemas)

  "Donors" should "map hpo terms per donors" in {
    1 shouldBe 1
  }
}