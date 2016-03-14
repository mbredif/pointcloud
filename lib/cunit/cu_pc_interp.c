/***********************************************************************
* cu_pc_interp.c
*
*        Testing for the schema API functions
*
***********************************************************************/

#include "CUnit/Basic.h"
#include "cu_tester.h"

/* GLOBALS ************************************************************/

static PCSCHEMA *schema = NULL;
static const char *xmlfile = "data/simple-schema.xml";
static const double precision = 0.000001;

// SIMPLE SCHEMA
// int32_t x
// int32_t y
// int32_t z
// int16_t intensity

/* Setup/teardown for this suite */
static int
init_suite(void)
{
	char *xmlstr = file_to_str(xmlfile);
	int rv = pc_schema_from_xml(xmlstr, &schema);
	pcfree(xmlstr);
	if ( rv == PC_FAILURE ) return 1;
	return 0;
}

static int
clean_suite(void)
{
	pc_schema_free(schema);
	return 0;
}

/* TESTS **************************************************************/

static void
test_interp_simple()
{
	// 00 endian (big)
	// 00000000 pcid
	// 00000000 compression
	// 00000002 npoints
	// 0000000800000003000000060006 pt1 (XYZi)
	// 0000000200000001000000040008 pt2 (XYZi)

	char *hexbuf = "0000000000000000000000000200000008000000030000000600060000000200000001000000040008";
	size_t hexsize = strlen(hexbuf);
	uint8_t *wkb = bytes_from_hexbytes(hexbuf, hexsize);
	PCPATCH *pa = pc_patch_from_wkb(schema, wkb, hexsize/2);
    PCPOINTLIST *li = pc_pointlist_from_patch(pa);

    PCPOINT *pt = pc_point_interp(pa, "X", 0.05, 0);
    char *pstr = pc_point_to_string(pt);
    CU_ASSERT_STRING_EQUAL(pstr, "{\"pcid\":0,\"pt\":[0.05,0.02,0.05,7]}");

    // free
    pcfree(wkb);
    pcfree(pstr);
    pc_patch_free(pa);
    pc_point_free(pt);
    pc_pointlist_free(li);
}

static void
test_interp_sorted()
{
	// 00 endian (big)
	// 00000000 pcid
	// 00000000 compression
	// 00000002 npoints
	// 0000000200000001000000040008 pt1 (XYZi)
	// 0000000800000003000000060006 pt2 (XYZi)

    char *pstr;
	char *hexbuf = "0000000000000000000000000200000002000000010000000400080000000800000003000000060006";
	size_t hexsize = strlen(hexbuf);
	uint8_t *wkb = bytes_from_hexbytes(hexbuf, hexsize);
	PCPATCH *pa = pc_patch_from_wkb(schema, wkb, hexsize/2);
    PCPOINTLIST *li = pc_pointlist_from_patch(pa);

    PCPOINT *pt = pc_point_interp(pa, "X", 0.05, 0);
    pstr = pc_point_to_string(pt);
    CU_ASSERT_STRING_EQUAL(pstr, "{\"pcid\":0,\"pt\":[0.05,0.02,0.05,7]}");
    pcfree(pstr);
    pc_point_free(pt);

    pt = pc_point_interp(pa, "X", 0.05, 1);
    pstr = pc_point_to_string(pt);
    CU_ASSERT_STRING_EQUAL(pstr, "{\"pcid\":0,\"pt\":[0.05,0.02,0.05,7]}");

    // free
    pcfree(wkb);
    pcfree(pstr);
    pc_patch_free(pa);
    pc_point_free(pt);
    pc_pointlist_free(li);
}

static void
test_interp_limits()
{
	// 00 endian (big)
	// 00000000 pcid
	// 00000000 compression
	// 00000002 npoints
	// 0000000800000003000000060006 pt1 (XYZi)
	// 0000000200000001000000040008 pt2 (XYZi)

	char *hexbuf = "0000000000000000000000000200000008000000030000000600060000000200000001000000040008";
	size_t hexsize = strlen(hexbuf);
	uint8_t *wkb = bytes_from_hexbytes(hexbuf, hexsize);
	PCPATCH *pa = pc_patch_from_wkb(schema, wkb, hexsize/2);
    PCPOINTLIST *li = pc_pointlist_from_patch(pa);
    char *pstr;
    
    PCPOINT *pt = pc_point_interp(pa, "X", 0.08, 0);
    CU_ASSERT_PTR_NULL_FATAL(pt);

    pt = pc_point_interp(pa, "X", 0.081, 0);
    CU_ASSERT_PTR_NULL_FATAL(pt);

    pt = pc_point_interp(pa, "X", 0.02, 0);
    pstr = pc_point_to_string(pt);
    CU_ASSERT_STRING_EQUAL(pstr, "{\"pcid\":0,\"pt\":[0.02,0.01,0.04,8]}");
    pcfree(pstr);
    pc_point_free(pt);

    pt = pc_point_interp(pa, "X", 0.019, 0);
    CU_ASSERT_PTR_NULL_FATAL(pt);

    // free
    pcfree(wkb);
    pc_pointlist_free(li);
    pc_patch_free(pa);
}

/* REGISTER ***********************************************************/

CU_TestInfo interp_tests[] = {
	PC_TEST(test_interp_simple),
	PC_TEST(test_interp_limits),
	PC_TEST(test_interp_sorted),
    CU_TEST_INFO_NULL
};

CU_SuiteInfo interp_suite = {"interp", init_suite, clean_suite, interp_tests};
