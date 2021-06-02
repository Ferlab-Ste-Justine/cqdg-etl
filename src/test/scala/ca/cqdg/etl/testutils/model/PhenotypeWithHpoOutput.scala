/**
 * Generated by [[bio.ferlab.datalake.spark3.ClassGenerator]]
 * on 2021-05-25T13:32:49.738754
 */
package ca.cqdg.etl.testutils.model


case class PhenotypeWithHpoOutput(`study_id`: String = "ST0001",
                                  `submitter_donor_id`: String = "PT00001",
                                  `phenotypes`: List[PHENOTYPES] = List(PHENOTYPES(
                                    `phenotype_id` = "HP:0001694",
                                    `name` = "Right-to-left shunt",
                                    `parents` = List("Cardiac shunt (HP:0001693)"),
                                    `age_at_phenotype` = Set(63),
                                    `phenotype_observed_bool` = true,
                                    `is_leaf` = true,
                                    `is_tagged` = true),
                                    PHENOTYPES(
                                      `phenotype_id` = "HP:0001626",
                                      `name` = "Abnormality of the cardiovascular system",
                                      `parents` = List("Phenotypic abnormality (HP:0000118)"),
                                      `age_at_phenotype` = Set(63),
                                      `phenotype_observed_bool` = true,
                                      `is_leaf` = false,
                                      `is_tagged` = false)))
