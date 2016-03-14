/***********************************************************************
* pc_interp.c
*
*  Pointclound patch linear interpolation.
*
*  PgSQL Pointcloud is free and open source software provided
*  by the Government of Canada
*  Copyright (c) 2013 Natural Resources Canada
*
*  Author: M. Brédif
*
***********************************************************************/

#include "pc_api_internal.h"
//#include <stdint.h>
#include <assert.h>

int
pc_compare_uncompressed_interp(const void *a, const void *b, void *arg)
{
  uint32_t interpretation = *(uint32_t *)arg;
  double da = *(double *)a;
  double db = pc_double_from_ptr(b,interpretation);
  return ((da > db) - (da < db));
}

// bsearch like binary search for a lower bound within a sorted range, returns base if key is below
// adapted from https://github.com/ngg/barelibc/blob/master/src/stdlib/bsearch.c
void * blower_arg(
        const void *key,
        const void *base0,
        size_t num,
        size_t size,
        int (*compar)(const void *, const void *, void *),
        void *arg)
{
    const char *base = base0;
    size_t lim;
    int cmp;
    const void *p;
    
    if (!num || (*compar)(key, base0, arg)<0) return NULL;
    for (lim = num; lim > 1; lim >>= 1) {
        p = base + (lim >> 1) * size;
        cmp = (*compar)(key, p, arg);
        if (cmp == 0)
            return ((void *)p);
        else if (cmp > 0) {	/* key > p: move right */
            base = (char *)p;
            lim++;
        } 		/* else move left */
    }
    return base;
}


// reports warnings if interpolated columns features duplicate values, proceeding with the interpolation using one of the duplicate points.
// should we fail on such ambiguous cases and require distinct values in the interpolated column ?
// queries outside of the patch value range (min inclusive, max exclusive) returns NULL
// returns null if patch is empty
uint8_t*
pc_point_uncompressed_interp_ptr(uint8_t *buf, uint8_t *data, uint32_t npoints, const PCSCHEMA *schema, PCDIMENSION *dim, double value, char sorted)
{
    int i = 0;
    size_t sz = schema->size;
    uint32_t interpretation = dim->interpretation;
    uint32_t byteoffset     = dim->byteoffset;
    uint8_t *buf0 = NULL;
    uint8_t *buf1 = NULL;
    double v = pc_value_unscale_unoffset(value,dim);
    double vi, v0, v1, t0, t1;

    if(!npoints) return NULL;
    data += byteoffset;
    
    if(sorted) {
        buf0 = (uint8_t *)blower_arg(&v,data,npoints,sz,pc_compare_uncompressed_interp,&interpretation);
        buf1 = buf0 + sz;
        if(!buf0 || buf1 == data + npoints*sz) {
            return NULL;
        }
        v0 = pc_double_from_ptr(buf0,interpretation);
        v1 = pc_double_from_ptr(buf1,interpretation);
        if(!(v0<=v && v<v1) ) {
            pcerror("Interpolated column %s is not strictly increasing",dim->name);
            return NULL;
        }
        
    } else {
        while ( i < npoints )
        {
            vi = pc_double_from_ptr(data,interpretation);
            if(vi<=v) {
                if(buf0 && vi==v0) pcwarn("Interpolated column %s features duplicate values",dim->name);
                if(!buf0 || vi>v0) { v0=vi; buf0=data; }
            } else {
                if(buf1 && vi==v1) pcwarn("Interpolated column %s features duplicate values",dim->name);
                if(!buf1 || vi<v1) { v1=vi; buf1=data; }
            }
            data += sz;
            i++;
        }
        if(!buf0 || !buf1) {
            return NULL;
        }
    }
    buf0 -= byteoffset; // point to point start
    if(v == v0) {
       memcpy(buf,buf0,sz);
       return buf0;
    }
    buf1 -= byteoffset; // point to point start
    t0 = (v1-v)/(v1-v0);
    t1 = 1.-t0;
    if(!pc_double_to_ptr(buf+byteoffset,interpretation,v))
    {
            return NULL;
    }
    for(i=0;i<schema->ndims;++i)
    {
        if(i==dim->position) continue;
        interpretation = schema->dims[i]->interpretation;
        byteoffset     = schema->dims[i]->byteoffset;
        v0 = pc_double_from_ptr(buf0+byteoffset,interpretation);
        v1 = pc_double_from_ptr(buf1+byteoffset,interpretation);
        if(!pc_double_to_ptr(buf+byteoffset,interpretation,t0*v0+t1*v1))
           return NULL;
    }
    return buf0;
}

static PCPOINT *
pc_point_uncompressed_interp(const PCPATCH_UNCOMPRESSED *pu, PCDIMENSION *dim, double value, char sorted)
{
    PCPOINT *pt = pc_point_make(pu->schema);
    if(pc_point_uncompressed_interp_ptr(pt->data,pu->data,pu->npoints,pu->schema,dim,value,sorted)) return pt;

    pc_point_free(pt);
    return NULL;
}

static PCPATCH *
pc_patch_uncompressed_interp(const PCPATCH_UNCOMPRESSED *pu1, const PCPATCH_UNCOMPRESSED *pu2, 
    PCDIMENSION *dim1, PCDIMENSION *dim2, char sorted1, char sorted2)
{
    char sorted = sorted1 && sorted2;
    PCPATCH_UNCOMPRESSED *pa = pc_patch_uncompressed_make(pu1->schema,pu2->npoints);
    int i;
    double value2;
    size_t sz1 = pu1->schema->size;
    size_t sz2 = pu2->schema->size;
    uint8_t *buf  = pa->data;
    uint8_t *buf1 = pu1->data;
    uint8_t *buf2 = pu2->data+dim2->byteoffset;
    uint8_t *ptr;
    uint32_t npoints1 = pu1->npoints;
    
    for (i=0; i<pu2->npoints;++i)
    {
        value2 = pc_value_from_ptr(buf2,dim2);
        if(ptr = pc_point_uncompressed_interp_ptr(buf,buf1,npoints1,pu1->schema,dim1,value2,sorted1))
        {
            if(sorted)
            {
                npoints1 -= (ptr-buf1) / sz1;
                buf1 = ptr;
            }
            buf += sz1;
            pa->npoints++;
        }
        buf2 += sz2;
    }
    
    if(pa->npoints) return (PCPATCH *)pa;
    
    pc_patch_free(pa);
    return NULL;    
}

PCPOINT *
pc_point_interp(const PCPATCH *pa, const char *name, double value, char sorted)
{
    PCDIMENSION *dim = pc_schema_get_dimension_by_name(pa->schema, name);
    if ( ! dim ) {
        pcerror("dimension \"%s\" does not exist", name);
        return NULL;
    }
    PCPATCH *pu = pc_patch_uncompress(pa);
    if ( !pu ) {
        pcerror("Patch uncompression failed");
        return NULL;
    }
    PCPOINT *pt = pc_point_uncompressed_interp((PCPATCH_UNCOMPRESSED *)pu,dim,value,sorted);
    if ( pu != pa )
        pc_patch_free(pu);
    return pt;
}


/** optimized PC_Patch(PC_Interpolate(p1,attr1,PC_Get(pt2,attr2), sorted1)) from PC_explode(p2) as pt2 */
PCPATCH *pc_patch_interp(
    const PCPATCH *p1, const PCPATCH *p2,
    const char *name1, const char *name2,
    char sorted1, char sorted2)
{
    PCDIMENSION *dim1 = pc_schema_get_dimension_by_name(p1->schema, name1);
    if ( ! dim1 ) {
        pcerror("dimension \"%s\" does not exist", name1);
        return NULL;
    }
    PCDIMENSION *dim2 = pc_schema_get_dimension_by_name(p2->schema, name2);
    if ( ! dim2 ) {
        pcerror("dimension \"%s\" does not exist", name2);
        return NULL;
    }
    PCPATCH *pu1 = pc_patch_uncompress(p1);
    if ( !pu1 ) {
        pcerror("Patch uncompression failed");
        return NULL;
    }
    PCPATCH *pu2 = pc_patch_uncompress(p2);
    if ( !pu2 ) {
        pc_patch_free(pu1);
        pcerror("Patch uncompression failed");
        return NULL;
    }
    PCPATCH *pa = pc_patch_uncompressed_interp((PCPATCH_UNCOMPRESSED *)pu1,(PCPATCH_UNCOMPRESSED *)pu2,dim1,dim2,sorted1,sorted2);
    if ( pu1 != p1 )
        pc_patch_free(pu1);
    if ( pu2 != p2 )
        pc_patch_free(pu2);

	if ( pa && PC_FAILURE == pc_patch_uncompressed_compute_extent(pa) )
	{
        pc_patch_free(pa);
		pcerror("%s: bounds computation failed", __func__);
		return NULL;
	}
	
	if ( pa && PC_FAILURE == pc_patch_uncompressed_compute_stats(pa) )
	{
        pc_patch_free(pa);
		pcerror("%s: stats computation failed", __func__);
		return NULL;
	}

    return pa;
}
