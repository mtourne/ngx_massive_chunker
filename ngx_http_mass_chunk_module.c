/**
 * Copyright (C) 2011 Matthieu Tourne
 * @author Matthieu Tourne <matthieu@cloudflare.com>
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

typedef struct {
    ngx_flag_t          enable;
    size_t              max_size;
    size_t              max_chunks;
} ngx_http_mass_chunk_loc_conf_t;

typedef struct {
    /* IO */
    ngx_chain_t         *free;
    ngx_chain_t         *busy;
    ngx_chain_t         *out;
    ngx_chain_t         **last_out;
} ngx_http_mass_chunk_ctx_t;

static ngx_int_t ngx_http_mass_chunk_init(ngx_conf_t *cf);

static ngx_int_t ngx_http_mass_chunk_header_filter(ngx_http_request_t *r);
static ngx_int_t ngx_http_mass_chunk_body_filter(ngx_http_request_t *r,
                                               ngx_chain_t *in);

static void *ngx_http_mass_chunk_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_mass_chunk_merge_loc_conf(ngx_conf_t *cf,
                                              void *parent, void *child);

static ngx_command_t ngx_http_mass_chunk_commands[] = {
    { ngx_string("mass_chunk"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
      |NGX_HTTP_LIF_CONF|NGX_CONF_FLAG,
      ngx_conf_set_flag_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_mass_chunk_loc_conf_t, enable),
      NULL },

    { ngx_string("mass_chunk_max_size"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
      |NGX_HTTP_LIF_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_mass_chunk_loc_conf_t, max_size),
      NULL },

    { ngx_string("mass_chunk_max_chunks"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
      |NGX_HTTP_LIF_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_size_slot,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_mass_chunk_loc_conf_t, max_chunks),
      NULL },

    ngx_null_command
};


static ngx_http_module_t ngx_http_mass_chunk_module_ctx = {
    NULL,                               /* preconfiguration */
    ngx_http_mass_chunk_init,		/* postconfiguration */

    NULL,                               /* create main configuration */
    NULL,                               /* init main configuration */

    NULL,                               /* create server configuration */
    NULL,                               /* merge server configuration */

    ngx_http_mass_chunk_create_loc_conf,/* create location configuration */
    ngx_http_mass_chunk_merge_loc_conf  /* merge location configuration */
};


ngx_module_t  ngx_http_mass_chunk_module = {
    NGX_MODULE_V1,
    &ngx_http_mass_chunk_module_ctx,    /* module context */
    ngx_http_mass_chunk_commands,       /* module directives */
    NGX_HTTP_MODULE,                    /* module type */
    NULL,                               /* init master */
    NULL,                               /* init module */
    NULL,                               /* init process */
    NULL,                               /* init thread */
    NULL,                               /* exit thread */
    NULL,                               /* exit process */
    NULL,                               /* exit master */
    NGX_MODULE_V1_PADDING
};

static ngx_http_output_header_filter_pt  ngx_http_next_header_filter;
static ngx_http_output_body_filter_pt    ngx_http_next_body_filter;

static ngx_int_t ngx_http_mass_chunk_header_filter(ngx_http_request_t *r) {
    ngx_http_mass_chunk_loc_conf_t      *lcf;
    ngx_http_mass_chunk_ctx_t           *ctx;

    lcf = ngx_http_get_module_loc_conf(r, ngx_http_mass_chunk_module);

    if (!lcf->enable || !lcf->max_size) {
        return ngx_http_next_header_filter(r);
    }

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "massive chunker filter");

    ctx = ngx_pcalloc(r->pool, sizeof (ngx_http_mass_chunk_ctx_t));
    if (ctx == NULL) {
        return NGX_ERROR;
    }
    ctx->last_out = &ctx->out;

    ngx_http_set_ctx(r, ctx, ngx_http_mass_chunk_module);

    /* loads the content of the response in : in->buf */
    r->filter_need_in_memory = 1;

    return ngx_http_next_header_filter(r);
}


static ngx_int_t
ngx_http_mass_chunk_body_filter(ngx_http_request_t *r, ngx_chain_t *in) {
    ngx_http_mass_chunk_loc_conf_t      *lcf;
    ngx_http_mass_chunk_ctx_t           *ctx;
    ngx_chain_t                         *cl;
    u_char                              *copy_start;
    u_char                              *copy_end;
    ngx_buf_t                           *b;
    ngx_int_t                           rc;
    ngx_int_t                           num;

    ctx = ngx_http_get_module_ctx(r, ngx_http_mass_chunk_module);
    if (ctx == NULL) {
        return ngx_http_next_body_filter(r, in);
    }

    lcf = ngx_http_get_module_loc_conf(r, ngx_http_mass_chunk_module);

    num = 1;

    while (in) {

        copy_start = in->buf->pos;
        copy_end = in->buf->pos;

        b = NULL;

        while (copy_end < in->buf->last) {

            copy_start = copy_end;

            if (copy_start > in->buf->last) {
                return NGX_ERROR;
            }

            copy_end = ngx_min(copy_start + lcf->max_size, in->buf->last);

            /* create buf */
            cl = ngx_chain_get_free_buf(r->pool, &ctx->free);
            if (cl == NULL) {
                return NGX_ERROR;
            }
            b = cl->buf;

            /* copy attributes */
            ngx_memcpy(b, in->buf, sizeof(ngx_buf_t));

            b->pos = copy_start;
            b->last = copy_end;

            b->shadow = NULL;
            b->last_buf = 0;
            b->recycled = 0;
            /* b->flush = 1; */

            if (b->in_file) {
                b->file_last = b->file_pos + (b->last - in->buf->pos);
                b->file_pos += b->pos - in->buf->pos;
            }

            *ctx->last_out = cl;
            ctx->last_out = &cl->next;

            if (lcf->max_chunks && num++ % lcf->max_chunks == 0) {
                if (ngx_http_next_body_filter(r, ctx->out) != NGX_OK) {
                    return NGX_ERROR;
                }

                ngx_chain_update_chains(r->pool, &ctx->free, &ctx->busy, &ctx->out,
                                        (ngx_buf_tag_t) &ngx_http_mass_chunk_module);

                ctx->last_out = &ctx->out;
            }

        }

        if (in->buf->last_buf) {
            cl = ngx_chain_get_free_buf(r->pool, &ctx->free);
            if (cl == NULL) {
                return NGX_ERROR;
            }
            b = cl->buf;


            *ctx->last_out = cl;
            ctx->last_out = &cl->next;

            b->last_buf = 1;
         }

        in->buf->pos = in->buf->last;
        in->buf->file_pos = in->buf->file_last;
        in = in->next;
    }

    if (ctx->out == NULL && ctx->busy == NULL) {
        return NGX_OK;
    }

    rc = ngx_http_next_body_filter(r, ctx->out);

    ngx_chain_update_chains(r->pool, &ctx->free, &ctx->busy, &ctx->out,
                            (ngx_buf_tag_t) &ngx_http_mass_chunk_module);

    ctx->last_out = &ctx->out;

    return rc;
}

static void*
ngx_http_mass_chunk_create_loc_conf(ngx_conf_t *cf) {
    ngx_http_mass_chunk_loc_conf_t      *lcf;

    lcf = ngx_pcalloc(cf->pool, sizeof(ngx_http_mass_chunk_loc_conf_t));
    if (lcf == NULL) {
        return NULL;
    }

    /*
     * set by ngx_pcalloc():
     *
     */

    lcf->enable = NGX_CONF_UNSET;
    lcf->max_size = NGX_CONF_UNSET_SIZE;
    lcf->max_chunks = NGX_CONF_UNSET_SIZE;

    return lcf;
}

static char*
ngx_http_mass_chunk_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child) {
    ngx_http_mass_chunk_loc_conf_t        *prev = parent;
    ngx_http_mass_chunk_loc_conf_t        *conf = child;

    ngx_conf_merge_value(conf->enable, prev->enable, 0);

    ngx_conf_merge_size_value(conf->max_size, prev->max_size, 10);
    ngx_conf_merge_size_value(conf->max_chunks, prev->max_chunks, 0);

    return NGX_CONF_OK;
}

static ngx_int_t
ngx_http_mass_chunk_init(ngx_conf_t *cf) {

    ngx_http_next_header_filter = ngx_http_top_header_filter;
    ngx_http_top_header_filter = ngx_http_mass_chunk_header_filter;

    ngx_http_next_body_filter = ngx_http_top_body_filter;
    ngx_http_top_body_filter = ngx_http_mass_chunk_body_filter;

    return NGX_OK;
}
