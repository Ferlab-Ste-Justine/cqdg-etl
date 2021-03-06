/**
 * Generated by [[bio.ferlab.datalake.spark3.ClassGenerator]]
 * on 2021-05-21T07:55:48.005322
 */
package ca.cqdg.etl.testutils.model


case class TreatmentOutput(`submitter_donor_id`: String = "PT00001",
                           `study_id`: String = "ST0001",
                           `treatments_per_donor_per_study`: List[TREATMENTS_PER_DONOR_PER_STUDY] = List(TREATMENTS_PER_DONOR_PER_STUDY()))

case class TREATMENTS_PER_DONOR_PER_STUDY(`submitter_treatment_id`: String = "TX00001",
                                          `submitter_diagnosis_id`: String = "DI00001",
                                          `treatment_type`: String = "Surgery",
                                          `diagnosis_ICD_term`: String = "Acute myocardial infarction",
                                          `treatment_is_primary`: String = "Yes",
                                          `treatment_intent`: String = "Curative",
                                          `treatment_response`: String = "Not applicable",
                                          `medication_name`: Option[String] = None,
                                          `medication_code`: Option[String] = None,
                                          `medication_class`: Option[String] = None,
                                          `medication_start_date`: Option[String] = None,
                                          `medication_end_date`: Option[String] = None)
