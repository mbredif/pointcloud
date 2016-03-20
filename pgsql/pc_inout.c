/***********************************************************************
* pc_inout.c
*
*  Input/output functions for points and patches in PgSQL.
*
*  PgSQL Pointcloud is free and open source software provided
*  by the Government of Canada
*  Copyright (c) 2013 Natural Resources Canada
*
***********************************************************************/

#include "pc_pgsql.h"      /* Common PgSQL support for our type */

/* In/out functions */
Datum pcpoint_in(PG_FUNCTION_ARGS);
Datum pcpoint_out(PG_FUNCTION_ARGS);
Datum pcpatch_in(PG_FUNCTION_ARGS);
Datum pcpatch_out(PG_FUNCTION_ARGS);

/* Typmod support */
Datum pc_typmod_in(PG_FUNCTION_ARGS);
Datum pc_typmod_out(PG_FUNCTION_ARGS);
Datum pc_typmod_pcid(PG_FUNCTION_ARGS);
Datum pcpatch_enforce_typmod(PG_FUNCTION_ARGS);
Datum pcpoint_enforce_typmod(PG_FUNCTION_ARGS);

/* Other SQL functions */
Datum pcschema_is_valid(PG_FUNCTION_ARGS);
Datum pcschema_get_ndims(PG_FUNCTION_ARGS);
Datum pcpoint_from_double_array(PG_FUNCTION_ARGS);
Datum pcpoint_from_record(PG_FUNCTION_ARGS);
Datum pcpoint_as_text(PG_FUNCTION_ARGS);
Datum pcpatch_as_text(PG_FUNCTION_ARGS);
Datum pcpoint_as_bytea(PG_FUNCTION_ARGS);
Datum pcpatch_bytea_envelope(PG_FUNCTION_ARGS);


static void
pcid_consistent(const uint32 pcid, const uint32 column_pcid)
{
	if ( column_pcid && pcid != column_pcid )
	{
		ereport(ERROR, (
			errcode(ERRCODE_DATATYPE_MISMATCH),
			errmsg("point/patch pcid (%u) does not match column pcid (%d)", pcid, column_pcid)
		));
	}
}


PG_FUNCTION_INFO_V1(pcpoint_in);
Datum pcpoint_in(PG_FUNCTION_ARGS)
{
	char *str = PG_GETARG_CSTRING(0);
	/* Datum pc_oid = PG_GETARG_OID(1); Not needed. */
	int32 typmod = 0;
	uint32 pcid = 0;
	PCPOINT *pt;
	SERIALIZED_POINT *serpt = NULL;

	if ( (PG_NARGS()>2) && (!PG_ARGISNULL(2)) )
	{
		typmod = PG_GETARG_INT32(2);
		pcid = pcid_from_typmod(typmod);
	}

	/* Empty string. */
	if ( str[0] == '\0' )
	{
		ereport(ERROR,(errmsg("pcpoint parse error - empty string")));
	}

	/* Binary or text form? Let's find out. */
	if ( str[0] == '0' )
	{
		/* Hex-encoded binary */
		pt = pc_point_from_hexwkb(str, strlen(str), fcinfo);
		pcid_consistent(pt->schema->pcid, pcid);
		serpt = pc_point_serialize(pt);
		pc_point_free(pt);
	}
	else
	{
		ereport(ERROR,(errmsg("parse error - support for text format not yet implemented")));
	}

	if ( serpt ) PG_RETURN_POINTER(serpt);
	else PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pcpoint_out);
Datum pcpoint_out(PG_FUNCTION_ARGS)
{
	PCPOINT *pcpt = NULL;
	PCSCHEMA *schema = NULL;
	SERIALIZED_POINT *serpt = NULL;
	char *hexwkb = NULL;

	serpt = PG_GETARG_SERPOINT_P(0);
	schema = pc_schema_from_pcid(serpt->pcid, fcinfo);
	pcpt = pc_point_deserialize(serpt, schema);
	hexwkb = pc_point_to_hexwkb(pcpt);
	pc_point_free(pcpt);
	PG_RETURN_CSTRING(hexwkb);
}


PG_FUNCTION_INFO_V1(pcpatch_in);
Datum pcpatch_in(PG_FUNCTION_ARGS)
{
	char *str = PG_GETARG_CSTRING(0);
	/* Datum geog_oid = PG_GETARG_OID(1); Not needed. */
	uint32 typmod = 0, pcid = 0;
	PCPATCH *patch;
	SERIALIZED_PATCH *serpatch = NULL;

	if ( (PG_NARGS()>2) && (!PG_ARGISNULL(2)) )
	{
		typmod = PG_GETARG_INT32(2);
		pcid = pcid_from_typmod(typmod);
	}

	/* Empty string. */
	if ( str[0] == '\0' )
	{
		ereport(ERROR,(errmsg("pcpatch parse error - empty string")));
	}

	/* Binary or text form? Let's find out. */
	if ( str[0] == '0' )
	{
		/* Hex-encoded binary */
		patch = pc_patch_from_hexwkb(str, strlen(str), fcinfo);
		pcid_consistent(patch->schema->pcid, pcid);
		serpatch = pc_patch_serialize(patch, NULL);
		pc_patch_free(patch);
	}
	else
	{
		ereport(ERROR,(errmsg("parse error - support for text format not yet implemented")));
	}

	if ( serpatch ) PG_RETURN_POINTER(serpatch);
	else PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pcpatch_out);
Datum pcpatch_out(PG_FUNCTION_ARGS)
{
	PCPATCH *patch = NULL;
	SERIALIZED_PATCH *serpatch = NULL;
	char *hexwkb = NULL;
	PCSCHEMA *schema = NULL;

	serpatch = PG_GETARG_SERPATCH_P(0);
	schema = pc_schema_from_pcid(serpatch->pcid, fcinfo);
	patch = pc_patch_deserialize(serpatch, schema);
	hexwkb = pc_patch_to_hexwkb(patch);
	pc_patch_free(patch);
	PG_RETURN_CSTRING(hexwkb);
}

PG_FUNCTION_INFO_V1(pcschema_is_valid);
Datum pcschema_is_valid(PG_FUNCTION_ARGS)
{
	bool valid;
	text *xml = PG_GETARG_TEXT_P(0);
	char *xmlstr = text_to_cstring(xml);
	PCSCHEMA *schema;
	int err = pc_schema_from_xml(xmlstr, &schema);
	pfree(xmlstr);

	if ( ! err )
	{
		PG_RETURN_BOOL(FALSE);
	}

	valid = pc_schema_is_valid(schema);
	pc_schema_free(schema);
	PG_RETURN_BOOL(valid);
}

PG_FUNCTION_INFO_V1(pcschema_get_ndims);
Datum pcschema_get_ndims(PG_FUNCTION_ARGS)
{
	int ndims;
	uint32 pcid = PG_GETARG_INT32(0);
	PCSCHEMA *schema = pc_schema_from_pcid(pcid, fcinfo);

	if ( ! schema )
		elog(ERROR, "unable to load schema for pcid = %d", pcid);

	ndims = schema->ndims;
	PG_RETURN_INT32(ndims);
}

/**
* pcpoint_from_double_array(integer pcid, float8[] returns PcPoint
*/
PG_FUNCTION_INFO_V1(pcpoint_from_double_array);
Datum pcpoint_from_double_array(PG_FUNCTION_ARGS)
{
	uint32 pcid = PG_GETARG_INT32(0);
	ArrayType *arrptr = PG_GETARG_ARRAYTYPE_P(1);
	int nelems;
	float8 *vals;
	PCPOINT *pt;
	PCSCHEMA *schema = pc_schema_from_pcid(pcid, fcinfo);
	SERIALIZED_POINT *serpt;

	if ( ! schema )
		elog(ERROR, "unable to load schema for pcid = %d", pcid);

	if ( ARR_ELEMTYPE(arrptr) != FLOAT8OID )
		elog(ERROR, "array must be of float8[]");

	if ( ARR_NDIM(arrptr) != 1 )
		elog(ERROR, "float8[] must have only one dimension");

	if ( ARR_HASNULL(arrptr) )
		elog(ERROR, "float8[] must not have null elements");

	nelems = ARR_DIMS(arrptr)[0];
	if ( nelems != schema->ndims || ARR_LBOUND(arrptr)[0] > 1 )
		elog(ERROR, "array dimensions do not match schema dimensions of pcid = %d", pcid);

	vals = (float8*) ARR_DATA_PTR(arrptr);
	pt = pc_point_from_double_array(schema, vals, nelems);

	serpt = pc_point_serialize(pt);
	pc_point_free(pt);
	PG_RETURN_POINTER(serpt);
}

/*

PG_FUNCTION_INFO_V1(pcpoint_record);
Datum pcpoint_record(PG_FUNCTION_ARGS)
{
	SERIALIZED_POINT *serpt;
	PCSCHEMA *schema;
	PCPOINT *pt;
	Datum *elems;
	int i;
	bool *nulls;
	bool raw;

	TupleDesc         tupdesc;
	HeapTuple         tuple;

	serpt = PG_GETARG_SERPOINT_P(0);
	schema = pc_schema_from_pcid(serpt->pcid, fcinfo);
	pt = pc_point_deserialize(serpt, schema);
	if ( ! pt ) PG_RETURN_NULL();

	if(get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "pcpoint_record: result type is not composite");
	BlessTupleDesc( tupdesc );

	elems = (Datum * )palloc(tupdesc->natts * sizeof(Datum) );
	nulls = palloc( tupdesc->natts * sizeof( bool ) );

	i = tupdesc->natts;
	while (i--) {
		Form_pg_attribute attr = tupdesc->attrs[i];
		char *attname = pcstrdup(NameStr(attr->attname));
		char *arg = strchr(attname,'(');
		char *name = strtrim(attname);
		elems[i] = (Datum) 0;
		nulls[i] = true;
		if(attr->attisdropped) continue;
		raw = false;
		while( arg )
		{
			char *end = strrchr(arg,')');
			*arg++ = 0;
			if(!end) elog(ERROR, "pcpoint_record: unmatched parentheses in \"%s\"",NameStr(attr->attname));
			*end = 0;
			name = strtrim(name);
			arg = strtrim(arg);
			if(*arg==0)
			{
				if(attr->atttypid != INT4OID )
				{
					elog(ERROR, "pcpoint_record: Oid of \"%s\" should be INT4OID",NameStr(attr->attname));
				}

				if(strcasecmp(name,"row" )==0) { elems[i] = UInt32GetDatum(0);
				} else if(strcasecmp(name,"pcid")==0) { elems[i] = UInt32GetDatum(schema->pcid);
				} else if(strcasecmp(name,"srid")==0) { elems[i] = UInt32GetDatum(schema->srid);
				} else {
					elog(ERROR, "pcpoint_record: \"%s\" not recognized in \"%s\"",name,NameStr(attr->attname));
				}

				if( raw )
				{
					elog(ERROR, "pcpoint_record: \"%s()\" does not have a raw value in \"%s\"",name,NameStr(attr->attname));
				}
				nulls[i] = false;
				arg = NULL;

			} else if(strcasecmp(name,"raw")==0) { raw = true;
			} else if(strcasecmp(name,"min")==0) {
			} else if(strcasecmp(name,"max")==0) {
			} else if(strcasecmp(name,"avg")==0) {
			} else if( *name != 0 )
			{
				elog(ERROR, "pcpatch_record: \"%s\" not recognized in \"%s\"",name,NameStr(attr->attname));
			}
			name = arg;
			if(name) arg = strchr(name,'(');
		}
		if(name)
		{
			Oid oid;
			PCDIMENSION *dim = pc_schema_get_dimension_by_name(pt->schema, name);
			if(!dim)
			{
				elog(ERROR, "pcpoint_record: dimension \"%s\" not found in \"%s\"",name,NameStr(attr->attname));
			}

			oid = raw ? pc_interpretation_oid(dim->interpretation) : FLOAT8OID;
			if(oid != attr->atttypid)
			{
				elog(ERROR, "pcpoint_record: Incorrect Oid for \"%s\" in \"%s\" (%d!=%d)",name,NameStr(attr->attname),oid,attr->atttypid);
			}
			nulls[i] = false;
			if(raw)
			{
				elems[i] = pc_datum_from_ptr(pt->data + dim->byteoffset, dim->interpretation);
			}
			else
			{
				elems[i] = Float8GetDatum(pc_point_get_double(pt,dim));
			}
		}
		pcfree(attname);
	}
	pc_point_free(pt);
	tuple = heap_form_tuple( tupdesc, elems, nulls );
	pfree( nulls );
	pfree( elems );
	PG_RETURN_DATUM( HeapTupleGetDatum( tuple ) );
}
*/

#include "funcapi.h"
#include "pc_api_internal.h"
#include "access/htup_details.h"
#include "utils/typcache.h"

/**
* pcpoint_from_record(integer pcid, record) returns PcPoint
*/
PG_FUNCTION_INFO_V1(pcpoint_from_record);
Datum pcpoint_from_record(PG_FUNCTION_ARGS)
{
	uint32 pcid = PG_GETARG_INT32(0);
	HeapTupleHeader tuple = PG_GETARG_HEAPTUPLEHEADER(1);
	PCPOINT *pt;
	PCSCHEMA *schema = pc_schema_from_pcid(pcid, fcinfo);
	SERIALIZED_POINT *serpt;
	bool isNull;
	Oid oid;
	Datum datum;
	int i;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupDesc;


	if ( ! schema )
		elog(ERROR, "unable to load schema for pcid = %d", pcid);
	pt = pc_point_make(schema);

	tupType = HeapTupleHeaderGetTypeId(tuple);
	tupTypmod = HeapTupleHeaderGetTypMod(tuple);
	tupDesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	i = tupDesc->natts;
	while (i--)
	{
		Form_pg_attribute attr = tupDesc->attrs[i];
		parsed_attname parsed = pcparse_attname(NameStr(attr->attname));
		PCDIMENSION *dim;
		HeapTupleData tmptup;
		char tmp;

		if(parsed.fun != PC_FUN_NONE) continue;
		tmp = parsed.attname[parsed.nattname];
		parsed.attname[parsed.nattname] = 0;
		dim = pc_schema_get_dimension_by_name(schema, parsed.attname);
		parsed.attname[parsed.nattname] = tmp;

		if(!dim)
		{
			elog(ERROR, "pcpoint_from_record: dimension \"%.*s\" not found in \"%s\"",parsed.nattname,parsed.attname,NameStr(attr->attname));
		}

		tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
		ItemPointerSetInvalid(&(tmptup.t_self));
		tmptup.t_tableOid = InvalidOid;
		tmptup.t_data = tuple;

		datum = heap_getattr(&tmptup,attr->attnum,tupDesc,&isNull);

		if(parsed.raw)
		{
			oid = pc_interpretation_oid(dim->interpretation);
			if(!isNull) memcpy(pt->data+dim->byteoffset, &datum, dim->size);
		} else {
			oid=FLOAT8OID;
			if(!isNull) pc_point_set_double(pt,dim,DatumGetFloat8(datum));
		}
		if(oid != attr->atttypid)
		{
			elog(ERROR, "pcpoint_from_record: Incorrect Oid for \"%.*s\" in \"%s\" (%d!=%d)",parsed.nattname,parsed.attname,NameStr(attr->attname),oid,attr->atttypid);
		}
	}

	serpt = pc_point_serialize(pt);
	pc_point_free(pt);
	ReleaseTupleDesc(tupDesc);
	PG_RETURN_POINTER(serpt);
}

PG_FUNCTION_INFO_V1(pcpoint_as_text);
Datum pcpoint_as_text(PG_FUNCTION_ARGS)
{
	SERIALIZED_POINT *serpt = PG_GETARG_SERPOINT_P(0);
	text *txt;
	char *str;
	PCSCHEMA *schema = pc_schema_from_pcid(serpt->pcid, fcinfo);
	PCPOINT *pt = pc_point_deserialize(serpt, schema);
	if ( ! pt )
		PG_RETURN_NULL();

	str = pc_point_to_string(pt);
	pc_point_free(pt);
	txt = cstring_to_text(str);
	pfree(str);
	PG_RETURN_TEXT_P(txt);
}

PG_FUNCTION_INFO_V1(pcpatch_as_text);
Datum pcpatch_as_text(PG_FUNCTION_ARGS)
{
	SERIALIZED_PATCH *serpatch = PG_GETARG_SERPATCH_P(0);
	text *txt;
	char *str;
	PCSCHEMA *schema = pc_schema_from_pcid(serpatch->pcid, fcinfo);
	PCPATCH *patch = pc_patch_deserialize(serpatch, schema);
	if ( ! patch )
		PG_RETURN_NULL();

	str = pc_patch_to_string(patch);
	pc_patch_free(patch);
	txt = cstring_to_text(str);
	pfree(str);
	PG_RETURN_TEXT_P(txt);
}

PG_FUNCTION_INFO_V1(pcpoint_as_bytea);
Datum pcpoint_as_bytea(PG_FUNCTION_ARGS)
{
	SERIALIZED_POINT *serpt = PG_GETARG_SERPOINT_P(0);
	uint8 *bytes;
	size_t bytes_size;
	bytea *wkb;
	size_t wkb_size;
	PCSCHEMA *schema = pc_schema_from_pcid(serpt->pcid, fcinfo);
	PCPOINT *pt = pc_point_deserialize(serpt, schema);

	if ( ! pt )
		PG_RETURN_NULL();

	bytes = pc_point_to_geometry_wkb(pt, &bytes_size);
	wkb_size = VARHDRSZ + bytes_size;
	wkb = palloc(wkb_size);
	memcpy(VARDATA(wkb), bytes, bytes_size);
	SET_VARSIZE(wkb, wkb_size);

	pc_point_free(pt);
	pfree(bytes);

	PG_RETURN_BYTEA_P(wkb);
}

PG_FUNCTION_INFO_V1(pcpatch_bytea_envelope);
Datum pcpatch_bytea_envelope(PG_FUNCTION_ARGS)
{
	uint8 *bytes;
	size_t bytes_size;
	bytea *wkb;
	size_t wkb_size;
	SERIALIZED_PATCH *serpatch = PG_GETHEADER_SERPATCH_P(0);
	PCSCHEMA *schema = pc_schema_from_pcid(serpatch->pcid, fcinfo);

	bytes = pc_patch_to_geometry_wkb_envelope(serpatch, schema, &bytes_size);
	wkb_size = VARHDRSZ + bytes_size;
	wkb = palloc(wkb_size);
	memcpy(VARDATA(wkb), bytes, bytes_size);
	SET_VARSIZE(wkb, wkb_size);

	pfree(bytes);

	PG_RETURN_BYTEA_P(wkb);
}

PG_FUNCTION_INFO_V1(pc_typmod_in);
Datum pc_typmod_in(PG_FUNCTION_ARGS)
{
	uint32 typmod = 0;
	Datum *elem_values;
	int n = 0;
	int i = 0;
	ArrayType *arr = (ArrayType *) DatumGetPointer(PG_GETARG_DATUM(0));

	if (ARR_ELEMTYPE(arr) != CSTRINGOID)
		ereport(ERROR,
			(errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
			errmsg("typmod array must be type cstring[]")));

	if (ARR_NDIM(arr) != 1)
		ereport(ERROR,
			(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
			errmsg("typmod array must be one-dimensional")));

	if (ARR_HASNULL(arr))
		ereport(ERROR,
			(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
			errmsg("typmod array must not contain nulls")));

	if (ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr)) > 1)
		ereport(ERROR,
			(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
			errmsg("typmod array must have one element")));

	deconstruct_array(arr,
		CSTRINGOID, -2, false, 'c', /* hardwire cstring representation details */
		&elem_values, NULL, &n);

	for (i = 0; i < n; i++)
	{
		if ( i == 0 ) /* PCID */
		{
			char *s = DatumGetCString(elem_values[i]);
			typmod = pg_atoi(s, sizeof(int32), '\0');
		}
	}

	PG_RETURN_INT32(typmod);
}

PG_FUNCTION_INFO_V1(pc_typmod_out);
Datum pc_typmod_out(PG_FUNCTION_ARGS)
{
	char *str = (char*)palloc(64);
	uint32 typmod = PG_GETARG_INT32(0);
	uint32 pcid = pcid_from_typmod(typmod);


	/* No PCID value? Then no typmod at all. Return empty string. */
	if ( ! pcid )
	{
		str[0] = '\0';
		PG_RETURN_CSTRING(str);
	}
	else
	{
		sprintf(str, "(%u)", pcid);
		PG_RETURN_CSTRING(str);
	}
}


PG_FUNCTION_INFO_V1(pc_typmod_pcid);
Datum pc_typmod_pcid(PG_FUNCTION_ARGS)
{
	uint32 typmod = PG_GETARG_INT32(0);
	uint32 pcid = pcid_from_typmod(typmod);
	if ( ! pcid )
		PG_RETURN_NULL();
	else
		PG_RETURN_INT32(pcid);
}


PG_FUNCTION_INFO_V1(pcpatch_enforce_typmod);
Datum pcpatch_enforce_typmod(PG_FUNCTION_ARGS)
{
	SERIALIZED_PATCH *arg = PG_GETARG_SERPATCH_P(0);
	uint32 typmod = PG_GETARG_INT32(1);
	uint32 pcid = pcid_from_typmod(typmod);
	/* We don't need to have different behavior based on explicitness. */
	/* bool isExplicit = PG_GETARG_BOOL(2); */

	/* Check if column typmod is consistent with the object */
	if ( pcid != arg->pcid )
		elog(ERROR, "column pcid (%d) and patch pcid (%d) are not consistent", pcid, arg->pcid);

	PG_RETURN_POINTER(arg);
}

PG_FUNCTION_INFO_V1(pcpoint_enforce_typmod);
Datum pcpoint_enforce_typmod(PG_FUNCTION_ARGS)
{
	SERIALIZED_POINT *arg = PG_GETARG_SERPOINT_P(0);
	int32 typmod = PG_GETARG_INT32(1);
	uint32 pcid = pcid_from_typmod(typmod);
	/* We don't need to have different behavior based on explicitness. */
	/* bool isExplicit = PG_GETARG_BOOL(2); */

	/* Check if column typmod is consistent with the object */
	if ( pcid != arg->pcid )
		elog(ERROR, "column pcid (%d) and point pcid (%d) are not consistent", pcid, arg->pcid);

	PG_RETURN_POINTER(arg);
}
