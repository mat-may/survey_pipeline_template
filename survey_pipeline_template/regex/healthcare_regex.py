from collections import namedtuple

from survey_pipeline_template.regex import match_with_exclusions

RegexPattern = namedtuple("RegexPattern", ["positive_regex_pattern", "negative_regex_pattern"])


# exclusions
teaching_exclusions = match_with_exclusions(
    r"EDUCATION|SCHOOL|BUSINESS SCHOOL|STUDYING|LECTURER|PROFESSOR|ACADEMIC|RESEARCH|UNIVERSITY|TEACH|STUDENT|TECHNICIAN|DOCTORATE|PHD|POSTDOCTORAL|AC[AE]DEMIC|RESEAR*CH|LAB(ORATORY)?|DATA|ANAL|STATIST|EPIDEMI|EXAM|EDUCAT|EARLY YEARS|COLL.GE|TEACH|LECTURE|PROFESS|HOUSE *(M[AI]ST(ER|RESS)|PARENT)|COACH|TRAIN|INSTRUCT|TUTOR|LEARN",
    "MEDICAL",
)
catering_exclusions = r"CHEF|SOUS|COOK|CATER|BREWERY|CHEESE|KITCHEN|KFC|CULINARY|FARM(ER|ING)"
media_exclusions = r"BROADCAST|JOURNALIST|CAMERA|WRIT|COMMUNICAT|CURAT(OR)*|MARKETING|MUSICIAN|ACT([OE]R|RESS)|ARTIST"
retail_exclusions = r"RETAIL|BUYER|SALE|BUY AND SELL|CUSTOMER|AGENT|BANK(ING|ER)|INSURANCE|BEAUT(Y|ICIAN)?|NAIL|HAIR|SHOP|PROPERTY|TRADE|SUPER *MARKET|WH *SMITH|TESCO"
domestic_exclusions = r"DOMESTIC|CLEAN|LAU*ND.*Y|HOUSE KEEPER"
call_exclusions = r"ADVI[SC]E|CALL|PHONE"
construction_exclusions = r"BUILD|CONSTRUCT|RENOVAT|REFIT|ENGINE|PLANT|CR[AI]*NE*|SURVEY(OR)*|DESIGNER|ARCHITECT|TECHNICIAN|MECHAN|MANUFACT|ELECTRIC|CARPENTER|PLUMB|WELD(ER|ING)|PASTER(ER|ING)|\bEE\b|GARDE*N|FURNITURE|MAINT[AIE]*N*[EA]N*CE|\bGAS\b|JOINER|DRAFT"
religious_exclusions = r"CHAPL[AI]*N|VICAR|CLERGY|MINISTER|PREACH|CHURCH"
it_exclusions = r"\bI[ \.]*T\.?\b|DIGIT|WEBSITE|NETWORK|DEVELOPER|SOFTWARE|SYSTEM|ONLINE|BUSINESS DEVELOPMENT"
public_service_exclusions = r"CHAIR|CHARITY|CITIZEN|CIVIL|VOLUNT|LIBRAR|TRANSLAT|INVESTIGAT|FIRE ?(WO)?(M[AE]N|FIGHT)|POLICE|POST *(WO)*MAN|PRISON|FIRST AID|SAFETY|\bTAX\b|LI[CS][EA]N[CS]E|LEA*GAL|LAWYER|\bLAW\b|SO*LICITOR"
transport_exclusions = (
    r"DRIV(E|ER|ING)|PILOT(?! (TRIAL|STUDY|TEST|MEDICAL))|TRAIN DRIVE|TAXI|LORRY|TRANSPORT|DELIVER|SUPPLY"
)
base_non_healthcare = r"AC[AE]DEMIC|LECTURE|DEAN|CURATOR|DOCTOR SCIENCE|DR LAB|DATA ANAL|AC?OUNT(ANT|ANCY)?|WARE *HOUSE|TRADE UNION|SALES (MANAGER|REP)|INVESTIGATION OF+ICE|AC+OUNT|PRISI?ON|DIRECT[OE]R|FINANC(IAL|E)"
nursing_or_residential_exclusions = r"(MUM|MOTHER|DAD|FATHER|SON|D[AU]+GHT|WIFE|HUSB|PARTNER|CHILD|FAM)|(DAY|ADULT|COMMUNITY) (CENT(RE|ER))|(CHILD|DAY)\s*(CARE)|NURSERY|(HOME CARER*)|(INDEPENDENT|SUPPORTED|DOMICILIARY) (CARE|LIVING|HOUSING)|(THEIR|OWN|CLIENT) (HOUSE|HOME)"

# base inclusions
base_inclusions = r"(PALLIATIVE|INTENSIVE) CARE|CHIROPRACT|PRIMARY HEALTCHARE"

vet = match_with_exclusions(r"\bVETS*|\bVEN?T[A-Z]*(RY|IAN)|EQUIN|\b(DOG|CAT)|HEDGEHOG|ANIMAL", r"VET PEOPLE|VETERAN")
hospital_generic = r"HOSPITAL(?!ITY)"
hc_admin = match_with_exclusions(
    [
        r"\bADMIN(?!IST[EO]R)|ADM[A-Z]{2,}RAT[EO]R|CLERICAL|CLERK",
        r"NHS|HOSPITAL|MEDICAL|SURG[EA]RY|CLINIC|HEALTH *CARE|CLINICAL *CODER|\bWARD *CLERK",
    ],
    r"STATIST|SCHOOL|LOCAL *GOVERNMENT|CIVIL *SERV(ANT|ICE)|^BANK CLERK|\bCHURCH\b",
)
hc_counsellor = match_with_exclusions(
    [r"COUN(S|C)|THERAP*(Y|IST)", r"ADDICT.*|VICTIM|TRAUMA|\sMENTAL HEALTH|DRUG|ALCOHOL|ABUSE|SUBSTANCE"],
    [
        r"REPRESENT|BUSINESS|POLI(C|T)|CAREER|DISTRICT|LOCAL|COUNTY|DEBT|CITY|COUNCIL\s|COUNCIL$|ACCOUNTANT|SOLICITOR|LAW|CHAPLAN|CHAPLAIN|DEFENCE|GOVERNMENT|PARISH|LAWYER|ASSESSOR|CURRICULUM|LEGAL|PRISONER|FARMER|CASEWORK|CARE WORK",
        teaching_exclusions,
    ],
)
hc_receptionist = match_with_exclusions(
    [
        r"RECEPTION|OPTICAL ASSISTANT|RECEPTION *(WORK|DUTIES)",
        r"NHS|HOSPITAL$|OSTEOPATH|OUTPATIENT|HOSPITAL(?!ITY)|MEDICAL|SURG[EA]RY|CLINIC|HEALTH *CARE|DENTAL|DENTIST|\bGP\b|\bDOCTOR|OPTICIAN|OPTICAL|CHIROPRAC|A&E",
    ],
    [
        r"SCHOOL|LOCAL *GOVERNMENT|CIVIL *SERV(ANT|ICE)|\bCHURCH\b|\bHOTEL|\bCARE *HOME|\bVET[A-Z]*RY\b|HAIR *(SALON|DRESS)+|EDUCATION|SPORT[S ]*CENT|LEISURE|BEAUTY|COLLEGE",
        r"\bSPA\b|RETAIL|\bLAW\b\|\bLEGAL|\bBAR WORK|GARAGE|\bVET[S]*\b",
        r"LOCAL *GOVERNMENT|CIVIL *SERV(ANT|ICE)|\bCHURCH\b|\bHOTEL",
    ],
)
hc_secretary = match_with_exclusions(
    [
        r"S.?C+R+.?T+.?R+Y|\sPA\s|P.?RS+.?N+.?L AS+IS+T+AN+",
        r"MEDIC.*|HEALTH|NHS|HOSPITAL\s|HOSPITAL$|CLINIC PATIENT|CAMHS|X.?RAY|\sDR\s|DOCTOR|PAEDIATRIC|A&E|DENTIST|PATIENT|GP|DENTAL|SURGERY|OPTI.*|OPTICAL|OPTICIANS",
    ],
    [
        r"LEGAL|LAW|SOLICITOR|PRODUCTION|PARISH|DOG|INTERNATIONAL|COMPANY|COMPANY|EDUCATION|UNIVERSITY|SCHOOL|TEACHER|FINANCE|BUILDER|BUSINESS|BANK|PROJECT|CHURCH|ESTATE AGENT|MANUFACT|SALE|SPORT|FARM|CLUB",
        r"CONTRACTOR|CIVIL SERV.*|CLERICAL|COUNCIL|MEDICAL SCHOOL|ACCOUNT|CARER|CHARITY",
    ],
)
hc_support = match_with_exclusions(
    [
        r"SUP+ORT *WORKER|ASSISTANT",
        r"HOSPITAL|HEALTH *CARE|MENTAL *HEALTH|MATERNITY(?! LEAVE)|CLINICAL|WARD|NURSE\b|NURSING(?! *HOME)|SURGERY|A&E|ONCOLOGY|PHLEBOTOM|AMBULANCE|WARD|MIDWIFE|IN *LABOUR|ACCIDENT *(& *|AND *)*EMERGENCY|COVID.*SWA[BP]",
    ],
    [r"BU(IS|SI)NESS SUPPORT", r"LOCAL COUNCIL|DISCHARGE|POST HOSPITAL|HOME NURS", r"HEALTH *CARE ASSIST|\bHCA\b"],
)
domestic = match_with_exclusions(
    r"(HOME|HOUSE|DOMESTIC) *CARE|CARER* OF HOME|HOUSE *WIFE|HOME *MAKER",
    r"(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|(?<!NO[NT][ -])MEDICAL|DONOR CARER*|HOSPITAL",
)
child_care = match_with_exclusions(
    r"CHILD *(CARE|MIND)|NANN[YIE]+\b|AU PAIR|(TODDLER|BABY|YOUTH|PLAY) *GROUP",
    r"(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|MEDICAL|DONOR CARER*|HOSPITAL",
)
informal_care = match_with_exclusions(
    r"(((CAR(E|ING)+|NURSE) (FOR|OF))|LOOKS* *AFTER) *(MUM|MOTHER|DAD|FATHER|SON|D[AU]+GHT|WIFE|HUSB|PARTNER|CHILD|FAM|T*H*E* ELDERLY)",
    r"(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|MEDICAL|DONOR CARER*" + "|" + call_exclusions,
)
house_care = match_with_exclusions(
    r"(HOME|HOUSE|DOMESTIC) *CARE|CARER* OF HOME|HOUSE *WIFE|HOME *MAKER",
    r"(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|(?<!NO[NT][ -])MEDICAL|DONOR CARER*",
)
residential_care = match_with_exclusions(
    r"(CARE|NURSING|RESIDENT[^\s]*|OLD PEOPLE[^\s]*|RETIRE[^\s]*|AGING CARE) *(HOME|FACILITY|CENTER|CENTRE|CARE|CAE?RE*R)",
    r"(?<!MENTAL )HEALTH *CARE|CRITICAL CARE|MEDICAL|COMM?UNITY|DONOR CARER*|HOSPITAL|AMBULANCE"
    + "|"
    + call_exclusions
    + "|"
    + nursing_or_residential_exclusions,
)
pharmacist = match_with_exclusions(
    [r"PHARMA(?![CS][EU]*TIC)", r"AS+IST|TECHN|RETAIL|DISPEN[SC]|SALES AS+IST|HOSPITAL|PRIMARY CARE|SERVE CUSTOM"],
    r"ANALYST|CARE HOME|ELECTRICAL|COMPAN(Y|IES)|INDUSTR|DIRECTOR|RESEARCH|WAREHOUSE|LAB|PROJECT|PRODUCTION|PROCESS|LAB|QA|QUALITY",
)
dietician = match_with_exclusions(r"\bD[EIA]{0,2}[TC][EI]?[CT]+[AEIOU]*[NC(RY)]|\bDIET(RIST)?\b", "DETECTION")
doctor = match_with_exclusions(
    r"(?<!WITH )DOCT[EO]R|\bGP\b|\Bdr\B|GENERAL PRACTI(CIAN|TION)|\bDR\b|\A ?(&|AND) ?E\|PHYSI[CT]I[AO]|MEDICAL EXAMINER",
    r"LECTURER|DOCTORI*AL|RESEARCH|PHD|STAT|WAR|ANIMAL|SALES*|FINANCE",
)
general_practitioner = (
    r"\bGP\b|(GENERAL|SURGERY) PRACTIT.*ER|SURGERY DOCTOR|GENERAL SURVEY|(DOCTOR.{0,2}|DR.{0,2}) SURGERY"
)
dentist = match_with_exclusions(r"DENTIS.*|\bDENTAL|ORAL HEALTH", "TECHNIC")
accident_and_emergency = r"A&E|ACCIDENT (&|AND) EMERGENCY|EMERGENCY ROOM"
midwife = r"MI*D*.?WI*F.?E.?|MIDWIV|MID*WIF|HEALTH VISITOR|MATERNITY (?!LEAVE)"
nurse = match_with_exclusions(
    r"(?<!WITH )N[IU]RS([EY]|ING)|MATRON|\bHCA\b", r"N[UI][RS]S[EA]R*[YIEU]+|(CARE|NURSING) *HOME|SCHOOL|TEACHER"
)
paramedic = match_with_exclusions("PARA *MEDIC|AMBUL[AE]NCE", teaching_exclusions)
covid_test = match_with_exclusions(
    ["COVID", "TEST|SWAB|VAC+INAT|IM+UNIS|SCREEN|WARD|JAB(\b|S)"], "LAB|AN[AY]LIST|SCHOOL|MANAGER"
)
therapist = match_with_exclusions(
    r"PH[YI]S[IY]CAL\s*REHAB|PH[YI]S[IY]CAL\s*THERAP*(Y|IST)|P(HY)?S[A-Z]*[IY]ST|(WHEEL *CHAIR|DISABILITY) THERAP(Y|IST)|PHYSIO\b|\bMENTAL HEALTH\b",
    r"EDUCATION|SCHOOL|BUSINESS|STUDYING|LECTURER|PROFESSOR|ACADEMIC|RESEARCH|PHYSICIST|UNIVERSITY|TEACHING|TEACH|STUDENT|TECHNICIAN|DOCTORATE|PHD|POSTDOCTORAL|PRISON|OCCUPATION|OCCUPATIONAL|FORENSICS|\bUCL\b",
)
hc_theatre = match_with_exclusions("THEATRE", "PERFORMER|ACTOR|STAGE|PLAY|CINEMA|MOVIE|WARDROBE|PRODUCER|DIRECTOR")
social_work = match_with_exclusions(
    r"SOCIAL.*WORK|FOSTER CARE|CHILD PROTECTION|(ADULT)?LEARNING DIFFICULT*|PROBATION OFFICER|OCC?UPATIONAL THERAP|\bOT\b|YOUTH OFFENDER|HMP|COMMUNITY WORKER|PRISON OFFIC|SHELTERED (ACC.*N|LIVING)",
    "SOCIAL(LY)? DISTANCE",
)
support_work = "SUP+ORT *WORKER"
apprentice = "AP*RENTI[CS]"
hc_call_operator = match_with_exclusions(
    [
        r"111|119|999|911|NHS|TRIAGE|EMERGENCY",
        r"ADVI[SC][OE]R|RESPONSE|OPERAT|CALL (HANDLER|CENT(RE|ER)|TAKE)|(TELE)?PHONE|TELE(PHONE)?|COVID",
    ],
    "CUSTOMER SERVICE|SALES|POLICE|ROADSIDE|\bAA\b|ATTEND",
)

additional_primary_hc = r"OUTPATIENT"
additional_secondary_hc = match_with_exclusions(
    r"ANA?ETH|(?<!TREE )SURGE(ON|ERY)|\bOPD\b|\bITU\b|(SPEECH|LANGUAGE) THERAPIST|MICROB.*IST|RADIO(GRAPHER|LOGIST|LOGY)|PHYSCOL.*IST|PAEDIATRI[CT]I[AO]N|SONOGRAPHER|VAC+INAT[OE]R|(DIABETIC EYE|RETINAL) SCRE+NER|(PH|F)LEBOTOM|CLINICAL SCIEN|MEDICAL PHYSICIST|CARDIAC PHYSIOLOG|OSTEOPATH|OPTOMOTRIST|PODIATRIST|O.?STETRI|GYNACOLOG|ORTHO(DOENT)?+|OPTI[TC]I[AO]N|CRITICAL CARE PRACTITIONER|HOSPITAL PORTER|PALLIATIVE|DISTRICT NURS|PAEDIATRI[CT]I[AO]N|HAEMATOLOGIST|PSYCHOLOG(IST|Y)|PROSTHETIST",
    r"EDUCATION|SCHOOL|BUSINESS|STUDYING|LECTURER|PROFESSOR|ACADEMIC|RESEARCH|PHYSICIST|UNIVERSITY|TEACHING|TEACH|STUDENT|TECHNICIAN|DOCTORATE|PHD|POSTDOCTORAL|PRISON|OCCUPATION|OCCUPATIONAL|FORENSICS|\bUCL\b",
)
other_hc = r"OPTICIAN|OPTHAL.*IST|ACCUP.*RE|THERAPIST|FOOT PRACTI(CIAN|TION)|HOMEOPATHY|PHYSCOT.*IST|\bMORT|HOSPITAL|\bNHS\b|MEDICAL EQUIP|TEACHING MEDICAL|WELLBEING PRACTITIONER"

support_roles = "ASSISTANT"  # supporting types of people

# patient facing
patient_facing_negative_regex = match_with_exclusions(
    r"(NO|LIMITED|NOT) FACE TO FACE|ONLINE|ZOOM|MICROSOFT|MS TEAMS|SKYPE|GOOGLE HANGOUTS?|REMOTE|VIRTUAL|DIGITAL|(ONLY|OVER THE) (TELE)?PHONE|((TELE)?PHONE|VIDEO) (CONSULT|CALL|WORK|SUPPORT)|(NO[TN]( CURRENTLY)?|NEVER) (IN PERSON|FACE TO FACE)|SH[EI]+LDING|WORK(ING)? (FROM|AT) HOME|HOME ?BASED|DELIVER(Y|ING)? PRESCRI|NO (CONTACT|VISITORS)",
    r"(?<!NOT )OFFICE BASED",
)
patient_facing_positive_regex = "|".join(
    [
        r"PALLIATIVE CARE|(?<!NOT )PATI[EA]NT FACING|(LOOK(S|ING)? AFTER|SEES?|CAR(E|ING) (OF|FOR)) PATI[EA]NTS|FACE TO FACE|FACE TO FACE",
        r"(?<!NO )(DIRECT )?CONTACT WITH PATI[EA]NTS|CLIENTS COME TO (HER|HIS|THEIR) HOUSE",
    ]
)

healthcare_classification = {
    "Primary": [
        "accident_and_emergency",
        "additional_primary_hc",
        "call_operator",
        "dentist",
        "general_practitioner",
        "paramedic",
        "pharmacist",
        "doctor",
        "nurse",
    ],
    "Secondary": [
        "additional_secondary_hc",
        "base_incluions",
        "dietician",
        "hc_theatre",
        "hospital_generic",
        "midwife",
        "hc_therapist",
        "base_healthcare",
        "covid_test",
        "hc_theatre",
        "hc_support",
    ],
    "Other": ["hc_counsellor", "hc_receptionist"],
}

social_care_classification = {
    "Other": ["social_work"],
    "Care/Residential home": ["residential_care"],
}

patient_facing_classification = {
    "Y": [
        "accident_and_emergency",
        "additional_primary_hc",
        "additional_secondary_hc",
        "base_inclusions",
        "dentist",
        "dietician",
        "doctor",
        "general_practitioner",
        "hc_counsellor",
        "hc_theatre",
        "hospital_generic",
        "midwife",
        "nurse",
        "hc_receptionist",
        "paramedic",
        "pharmacist",
        "hc_therapist",
        "social_work",
        "residential_care",
        "hc_support",
        "covid_test",
    ],
    "N": ["hc_call_operator", "hc_admin", "hc_secretary", "domestic"],
}

roles_map = {
    "additional_primary_hc": additional_primary_hc,
    "additional_secondary_hc": additional_secondary_hc,
    "hospital_generic": hospital_generic,
    "hc_admin": hc_admin,
    "hc_theatre": hc_theatre,
    "accident_and_emergency": accident_and_emergency,
    "hc_secretary": hc_secretary,
    "hc_receptionist": hc_receptionist,
    "hc_counsellor": hc_counsellor,
    "hc_support": hc_support,
    "pharmacist": pharmacist,
    "call_operator": hc_call_operator,
    "dietician": dietician,
    "doctor": doctor,
    "general_practitioner": general_practitioner,
    "dentist": dentist,
    "midwife": midwife,
    "nurse": nurse,
    "paramedic": paramedic,
    "hc_therapist": therapist,
    "covid_tester": covid_test,
    "base_healthcare": base_inclusions,
    "base_non_healthcare": base_non_healthcare,
    "retired": "RETIRED",
    "transport": transport_exclusions,
    "catering": catering_exclusions,
    "teaching": teaching_exclusions,
    "media": media_exclusions,
    "retail": retail_exclusions,
    "domestic": domestic_exclusions,
    "construction": construction_exclusions,
    "religion": religious_exclusions,
    "IT": it_exclusions,
    "public_service": public_service_exclusions,
    "vet": vet,
    "house_care": house_care,
    "child_care": child_care,
    "informal_care": informal_care,
    "residential_care": residential_care,
    "social_work": social_work,
    "apprentice": apprentice,
}

priority_map = {
    "vet": 3,
    "construction": 3,
    "retail": 3,
    "dentist": 3,
    "hospital_generic": 2,
    "paramedic": 0,
    "retired": 9,
    "teaching": 3,
    "media": 3,
    "hc_admin": 3,
    "hc_secretary": 3,
    "social_work": 4,
    "base_non_healthcare": 4,
    "public_service": 4,
    # "hospital_generic": 2,
    # "dentist": 3,
    "covid_tester": 3,
    "nurse": 2,
    "informal_care": 8,
    "therapist": 3,
}

patient_facing_pattern = RegexPattern(
    positive_regex_pattern=patient_facing_positive_regex, negative_regex_pattern=patient_facing_negative_regex
)
