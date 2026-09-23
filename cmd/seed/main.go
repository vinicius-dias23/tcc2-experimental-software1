// seed faz a carga inicial dos bancos a partir dos CSVs do dataset Olist.
//
// Os pedidos são ordenados por (order_purchase_timestamp, order_id) e a primeira
// fração (SEED_FRACTION, padrão 0.7) é gravada diretamente nas bases de origem e de
// destino, que partem assim do mesmo estado. A fração restante não é tocada aqui:
// ela é reproduzida como carga transacional pela API (scripts/carga), com a mesma
// regra de corte implementada em scripts/comum/olist.py.
//
// Clientes, vendedores e produtos são cadastros e entram inteiros na origem.
// A carga é idempotente: se a origem já tiver pedidos, nada é feito.
package main

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vinicius-dias23/tcc2-experimental-software1/internal/config"
	"github.com/vinicius-dias23/tcc2-experimental-software1/internal/pg"
)

// Nomes dos arquivos do dataset no Kaggle (olistbr/brazilian-ecommerce).
const (
	fileCustomers = "olist_customers_dataset.csv"
	fileSellers   = "olist_sellers_dataset.csv"
	fileProducts  = "olist_products_dataset.csv"
	fileOrders    = "olist_orders_dataset.csv"
	fileItems     = "olist_order_items_dataset.csv"
	filePayments  = "olist_order_payments_dataset.csv"
	// Marcador gravado por scripts/dados/gerar_amostra_sintetica.py.
	syntheticMarker = "_SINTETICO"
)

func main() {
	ctx := context.Background()
	dir := config.String("OLIST_DATA_DIR", "/data")
	fraction, err := strconv.ParseFloat(config.String("SEED_FRACTION", "0.7"), 64)
	if err != nil || fraction < 0 || fraction > 1 {
		log.Fatalf("SEED_FRACTION deve estar entre 0 e 1")
	}

	for _, f := range []string{fileCustomers, fileSellers, fileProducts, fileOrders, fileItems, filePayments} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			log.Fatalf("arquivo %s não encontrado em %s. Baixe o dataset Olist do Kaggle e extraia os CSVs "+
				"nessa pasta (ver README), ou gere a amostra sintética com scripts/dados/gerar_amostra_sintetica.py.", f, dir)
		}
	}
	_, statErr := os.Stat(filepath.Join(dir, syntheticMarker))
	synthetic := statErr == nil
	if synthetic {
		log.Printf("ATENÇÃO: %s contém a amostra SINTÉTICA, não o dataset real do Olist", dir)
	}

	origin := pg.Connect(ctx, config.String("ORIGIN_DATABASE_URL", "postgres://olist:olist@postgres-origem:5432/olist"), 4)
	defer origin.Close()
	dest := pg.Connect(ctx, config.String("DESTINATION_DATABASE_URL", "postgres://olist:olist@postgres-destino:5432/olist"), 4)
	defer dest.Close()

	var existing int
	if err := origin.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&existing); err != nil {
		log.Fatal(err)
	}
	if existing > 0 {
		log.Printf("origem já possui %d pedidos; carga inicial ignorada", existing)
		return
	}

	start := time.Now()
	orders := readCSV(dir, fileOrders)
	sort.SliceStable(orders.rows, func(i, j int) bool {
		a, b := orders.rows[i], orders.rows[j]
		ta, tb := a[orders.idx["order_purchase_timestamp"]], b[orders.idx["order_purchase_timestamp"]]
		if ta != tb {
			return ta < tb
		}
		return a[orders.idx["order_id"]] < b[orders.idx["order_id"]]
	})
	total := len(orders.rows)
	cut := int(math.Floor(float64(total) * fraction))
	seeded := orders.rows[:cut]
	seededIDs := make(map[string]bool, cut)
	for _, r := range seeded {
		seededIDs[r[orders.idx["order_id"]]] = true
	}
	log.Printf("%d pedidos no dataset; %d (%.0f%%) entram na carga inicial", total, cut, fraction*100)

	customers := readCSV(dir, fileCustomers)
	copyTable(ctx, origin, "customers",
		[]string{"customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"},
		customers, nil, textCols(5))
	copyTable(ctx, origin, "sellers",
		[]string{"seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"},
		readCSV(dir, fileSellers), nil, textCols(4))

	// O CSV original grafa "lenght"; aceitamos as duas formas.
	products := readCSV(dir, fileProducts)
	products.alias("product_name_length", "product_name_lenght")
	products.alias("product_description_length", "product_description_lenght")
	copyTable(ctx, origin, "products",
		[]string{"product_id", "product_category_name", "product_name_length", "product_description_length",
			"product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"},
		products, nil, []kind{text, text, integer, integer, integer, integer, integer, integer, integer})

	orders.rows = seeded
	orderCols := []string{"order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
		"order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"}
	orderKinds := []kind{text, text, text, timestamp, timestamp, timestamp, timestamp, timestamp}
	items := readCSV(dir, fileItems)
	itemCols := []string{"order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"}
	itemKinds := []kind{text, integer, text, text, timestamp, numeric, numeric}
	payments := readCSV(dir, filePayments)
	paymentCols := []string{"order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"}
	paymentKinds := []kind{text, integer, text, integer, numeric}
	onlySeeded := func(r []string, t *table) bool { return seededIDs[r[t.idx["order_id"]]] }

	for _, db := range []*pgxpool.Pool{origin, dest} {
		copyTable(ctx, db, "orders", orderCols, orders, nil, orderKinds)
		copyTable(ctx, db, "order_items", itemCols, items, onlySeeded, itemKinds)
		copyTable(ctx, db, "order_payments", paymentCols, payments, onlySeeded, paymentKinds)
	}

	if _, err := origin.Exec(ctx, `INSERT INTO seed_info (seed_fraction, orders_total, orders_seeded, synthetic)
		VALUES ($1, $2, $3, $4)`, fraction, total, cut, synthetic); err != nil {
		log.Fatal(err)
	}
	for _, db := range []*pgxpool.Pool{origin, dest} {
		if _, err := db.Exec(ctx, `ANALYZE`); err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("carga inicial concluída em %s", time.Since(start).Round(time.Millisecond))
}

type kind int

const (
	text kind = iota
	integer
	numeric
	timestamp
)

func textCols(n int) []kind { return make([]kind, n) }

type table struct {
	name string
	idx  map[string]int
	rows [][]string
}

func (t *table) alias(want, alt string) {
	if _, ok := t.idx[want]; !ok {
		if i, ok := t.idx[alt]; ok {
			t.idx[want] = i
		}
	}
}

func readCSV(dir, name string) *table {
	f, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.ReuseRecord = false
	header, err := r.Read()
	if err != nil {
		log.Fatalf("%s: %v", name, err)
	}
	t := &table{name: name, idx: map[string]int{}}
	for i, h := range header {
		// Remove BOM eventual do primeiro cabeçalho.
		if i == 0 && len(h) >= 3 && h[:3] == "\xef\xbb\xbf" {
			h = h[3:]
		}
		t.idx[h] = i
	}
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatalf("%s: %v", name, err)
		}
		t.rows = append(t.rows, rec)
	}
	return t
}

func copyTable(ctx context.Context, db *pgxpool.Pool, name string, cols []string, t *table,
	filter func([]string, *table) bool, kinds []kind) {
	for _, c := range cols {
		if _, ok := t.idx[c]; !ok {
			log.Fatalf("%s: coluna %s ausente", t.name, c)
		}
	}
	rows := make([][]any, 0, len(t.rows))
	for _, r := range t.rows {
		if filter != nil && !filter(r, t) {
			continue
		}
		row := make([]any, len(cols))
		for i, c := range cols {
			v, err := convert(r[t.idx[c]], kinds[i])
			if err != nil {
				log.Fatalf("%s: coluna %s: %v", t.name, c, err)
			}
			row[i] = v
		}
		rows = append(rows, row)
	}
	n, err := db.CopyFrom(ctx, pgx.Identifier{name}, cols, pgx.CopyFromRows(rows))
	if err != nil {
		log.Fatalf("copiando %s: %v", name, err)
	}
	log.Printf("%-15s %8d linhas (%s)", name, n, db.Config().ConnConfig.Host)
}

func convert(v string, k kind) (any, error) {
	if v == "" {
		return nil, nil
	}
	switch k {
	case integer:
		f, err := strconv.ParseFloat(v, 64) // o CSV traz inteiros como "1.0" em alguns campos
		if err != nil {
			return nil, err
		}
		return int32(f), nil
	case numeric:
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			return nil, err
		}
		return v, nil
	case timestamp:
		t, err := time.ParseInLocation("2006-01-02 15:04:05", v, time.UTC)
		if err != nil {
			return nil, err
		}
		return t, nil
	}
	return v, nil
}
