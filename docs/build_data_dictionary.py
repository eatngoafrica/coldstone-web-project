"""Builds docs/coldstone_web_data_dictionary.pdf from sql/redshift/01_tables_ng.sql plus the descriptions below.

Run from the project folder (needs reportlab):  python docs/build_data_dictionary.py sql/redshift/01_tables_ng.sql docs/coldstone_web_data_dictionary.pdf
When a table or column changes, update its description in TABLES below and rebuild."""
import re, sys
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, white
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from reportlab.platypus import (BaseDocTemplate, PageTemplate, Frame, Paragraph, Spacer, Table, TableStyle,
                                PageBreak, KeepTogether, CondPageBreak)

DDL, OUT = sys.argv[1], sys.argv[2]
INK, MUTED, ACCENT, LINE, HEAD_BG, ALT_BG = (HexColor(c) for c in
    ("#1f2933", "#5f6b7a", "#2f6db5", "#d5dbe3", "#e6f0fb", "#f6f8fa"))

S = {
    "title": ParagraphStyle("title", fontName="Helvetica-Bold", fontSize=24, leading=30, textColor=INK),
    "subtitle": ParagraphStyle("subtitle", fontName="Helvetica", fontSize=12, leading=17, textColor=MUTED),
    "h1": ParagraphStyle("h1", fontName="Helvetica-Bold", fontSize=16, leading=21, textColor=INK, spaceBefore=6, spaceAfter=8),
    "h2": ParagraphStyle("h2", fontName="Helvetica-Bold", fontSize=13, leading=17, textColor=ACCENT, spaceBefore=10, spaceAfter=4),
    "body": ParagraphStyle("body", fontName="Helvetica", fontSize=9.5, leading=13.5, textColor=INK, spaceAfter=5),
    "small": ParagraphStyle("small", fontName="Helvetica", fontSize=8.5, leading=11.5, textColor=INK),
    "smallb": ParagraphStyle("smallb", fontName="Helvetica-Bold", fontSize=8.5, leading=11.5, textColor=INK),
    "code": ParagraphStyle("code", fontName="Courier", fontSize=8, leading=10.5, textColor=INK),
    "cell": ParagraphStyle("cell", fontName="Helvetica", fontSize=8, leading=10.5, textColor=INK),
    "cellb": ParagraphStyle("cellb", fontName="Helvetica-Bold", fontSize=8, leading=10.5, textColor=INK),
    "cellm": ParagraphStyle("cellm", fontName="Courier", fontSize=7.5, leading=10, textColor=INK),
    "meta": ParagraphStyle("meta", fontName="Helvetica", fontSize=8.5, leading=12, textColor=MUTED),
}
P = lambda text, style="body": Paragraph(text, S[style])

# ---------------------------------------------------------------- table descriptions
PII = " <i>(personal data)</i>"
ACCOUNT = "ID of the Coldstone platform account that owns this record (the business account, not a shopper)."
TABLES = {
 # ---- Sales
 "orders": dict(group="Sales", grain="One row per customer order.",
  purpose="Every online order placed with Coldstone: who ordered, from which branch, how it was paid and delivered, its current status and its value.",
  refresh="Incremental. New and updated orders are added every day (D-1). Orders whose status changes later are updated in place, so each order always shows its latest state.",
  joins=["order_items, order_item_modifiers, order_timeline on order_external_id = orders.external_id",
         "branches on branch_external_id = branches.external_id",
         "customers on customer_external_id = customers.external_id"],
  cols={"external_id": "Order ID (unique).", "reference": "Human-readable order reference shown to customers and staff.",
   "payment_method": "How the order was paid. Values seen: card, wallet (website); Bank transfer, Cash, Card (call-centre orders). Note the mixed capitals.",
   "payment_status": "Payment state. Values seen: paid, pending. Pending orders are call-centre (cca) orders not yet confirmed as paid in the platform; many are paid by bank transfer.",
   "status": "Where the order is in its lifecycle. Values seen: preparing, ready, picked, delivered, rejected.",
   "channel": "Where the order was placed. Values seen: website, cca (call-centre orders, Nigeria only).",
   "subtotal": "Value of the items before delivery, in minor units (kobo / cents). Divide by 100 for naira / shillings.",
   "delivery_fee": "Delivery charge, in minor units.", "total_price": "Total charged (subtotal + delivery fee), in minor units.",
   "phone_number": "Contact phone number on the order." + PII,
   "branch_external_id": "Branch that fulfilled the order.", "branch_name": "Branch name as recorded on the order.",
   "created_at": "When the order was placed (UTC).", "updated_at": "When the order last changed, e.g. a status update (UTC).",
   "customer_external_id": "Customer who placed the order.", "customer_name": "Customer's name as recorded on the order." + PII,
   "delivery_type": "How the order reached the customer. Values seen: delivery, pickup."}),
 "order_items": dict(group="Sales", grain="One row per product line within an order.",
  purpose="The products in each order, with the price and quantity of each line.",
  refresh="Replaced with its order: when an order is loaded, all of its lines are deleted and re-inserted, so removed lines never linger.",
  joins=["orders on order_external_id = orders.external_id", "menu_items on product_external_id = menu_items.external_id",
         "order_item_modifiers on (order_external_id, line_no)"],
  cols={"order_external_id": "The order this line belongs to.", "line_no": "Position of the line within the order (1, 2, 3...).",
   "product_external_id": "Product ordered (a menu item).", "product_name": "Product name as recorded on the order.",
   "price": "Price recorded on the line, in minor units.", "quantity": "Number of units ordered."}),
 "order_item_modifiers": dict(group="Sales", grain="One row per option chosen on an order line.",
  purpose="Customisations picked for each product in an order, such as toppings, flavours and add-ons.",
  refresh="Replaced with its order, like order_items.",
  joins=["order_items on (order_external_id, line_no)", "orders on order_external_id = orders.external_id"],
  cols={"order_external_id": "The order.", "line_no": "The order line the option belongs to.",
   "modifier_no": "Position of the option on that line (1, 2, 3...).", "product_external_id": "Product on the line.",
   "modifier_external_id": "ID of the chosen option.", "modifier_name": "Name of the chosen option.",
   "price": "Extra charge for the option, in minor units (0 if free)."}),
 "order_timeline": dict(group="Sales", grain="One row per step in an order's progress.",
  purpose="The milestones each order goes through, for measuring preparation and delivery times.",
  refresh="Replaced with its order, like order_items.",
  joins=["orders on order_external_id = orders.external_id"],
  cols={"order_external_id": "The order.", "step_no": "Position of the step (1 = first).",
   "title": "Step name. Values seen: accepted, preparing, ready, shipped, delivered, picked.",
   "description": "Text describing the step.", "status": "True when the step has been reached; false when it hasn't (yet).",
   "created_at": "When the step was created (UTC).", "updated_at": "When the step last changed, e.g. was completed (UTC)."}),
 # ---- Customers
 "customers": dict(group="Customers", grain="One row per registered customer.",
  purpose="People who have registered or ordered on the Coldstone website.",
  refresh="Incremental. New and updated customers are added every day.",
  joins=["orders on orders.customer_external_id = customers.external_id",
         "branches on branch_external_id = branches.external_id"],
  cols={"external_id": "Customer ID (unique).", "first_name": "First name." + PII, "last_name": "Last name." + PII,
   "email": "Email address." + PII, "phone_number": "Phone number." + PII, "customer_external_id": ACCOUNT,
   "branch_external_id": "Branch the customer is registered with, if any.", "active": "True if the account is active.",
   "unsubscribed": "True if the customer has opted out of marketing messages.", "channel": "Where they signed up. Value seen: website.",
   "created_at": "When the customer registered (UTC).", "updated_at": "When their details last changed (UTC)."}),
 # ---- Money
 "wallet_transactions": dict(group="Wallet & settlement", grain="One row per money movement in a branch wallet.",
  purpose="Money coming into and out of each branch's wallet: sales credited, settlements paid out.",
  refresh="Incremental. New and updated transactions are added every day.",
  joins=["wallets on branch_external_id = wallets.branch_external_id", "branches on branch_external_id = branches.external_id"],
  cols={"external_id": "Transaction ID (unique).", "reference": "Transaction reference.",
   "amount": "Amount moved, in minor units.", "type": "Direction. Values seen: credit (money in), debit (money out).",
   "status": "Transaction state. Value seen: successful.", "customer_external_id": ACCOUNT,
   "branch_external_id": "Branch whose wallet moved.", "description": "What the movement was for. Values seen: Sales, Settlement.",
   "created_at": "When the transaction happened (UTC).", "updated_at": "When it last changed (UTC)."}),
 "wallets": dict(group="Wallet & settlement", grain="One row per branch wallet.",
  purpose="Each branch's wallet balance and the bank account its settlements are paid to.",
  refresh="Full snapshot. Reloaded in full every day, so balances are as at the latest run.",
  joins=["branches on branch_external_id = branches.external_id", "wallet_transactions on branch_external_id"],
  cols={"external_id": "Wallet ID (unique).", "reference": "Wallet reference.", "customer_external_id": ACCOUNT,
   "account_external_id": "ID of the linked account record in the platform.", "branch_external_id": "Branch that owns the wallet.",
   "balance": "Wallet balance, in minor units.", "current_balance": "Current balance as reported by the platform, in minor units.",
   "temp_balance": "Temporary / pending balance as reported by the platform, in minor units.", "active": "True if the wallet is active.",
   "account_name": "Settlement bank account name." + PII, "account_number": "Settlement bank account number." + PII,
   "bank_name": "Settlement bank.", "created_at": "When the wallet was created (UTC).", "updated_at": "When it last changed (UTC)."}),
 # ---- Menu
 "menu_items": dict(group="Menu", grain="One row per product.",
  purpose="Everything that can be sold: menu products and the products used as options (toppings, flavours, add-ons).",
  refresh="Full snapshot. Reloaded in full every day for every branch.",
  joins=["menu_categories on category_external_id = menu_categories.external_id", "branches on branch_external_id = branches.external_id",
         "menu_modifiers on menu_modifiers.menu_item_external_id = menu_items.external_id",
         "order_items on order_items.product_external_id = menu_items.external_id"],
  cols={"external_id": "Product ID (unique).", "slug": "URL-friendly product name.", "name": "Product name.",
   "description": "Product description.", "price": "Selling price, in minor units.", "original_price": "Price before any reduction, in minor units.",
   "lagos_price": "Lagos-specific price, in minor units (Nigeria only; empty when not used).",
   "regional_price": "Price outside Lagos, in minor units (Nigeria only; empty when not used).",
   "quantity": "Stock quantity, when stock is tracked.", "track_quantity": "True if stock levels are tracked.",
   "is_cup_size": "True if the product is a cup size option.",
   "cup_size": "Cold Stone size name, for size options. Values seen: like_it, love_it, gotta_have_it, ours, mine, everybody.",
   "has_sku": "True if the product has a stock-keeping unit code.", "sku": "Stock-keeping unit code.",
   "low_stock_threshold": "Stock level at which the product counts as running low.", "image": "Product image URL.",
   "channel": "Where the product is offered. Value seen: all.", "active": "True if the product is enabled.",
   "out_of_stock": "True if the product is currently marked out of stock.", "customer_external_id": ACCOUNT,
   "branch_external_id": "Branch whose menu the product belongs to.", "category_external_id": "Menu category.",
   "created_at": "When the product was created (UTC).", "updated_at": "When it last changed (UTC)."}),
 "menu_categories": dict(group="Menu", grain="One row per menu category.", purpose="Headings that group products on the menu.",
  refresh="Full snapshot. Reloaded in full every day.", joins=["menu_items on menu_items.category_external_id = menu_categories.external_id"],
  cols={"external_id": "Category ID (unique).", "name": "Category name."}),
 "menu_modifiers": dict(group="Menu", grain="One row per option group on a product.",
  purpose="The choices offered on a product, e.g. 'pick a flavour' or 'add toppings', and the rules for how many can be picked.",
  refresh="Full snapshot. Reloaded in full every day.",
  joins=["menu_items on menu_item_external_id = menu_items.external_id", "modifier_items on modifier_items.modifier_external_id = menu_modifiers.external_id"],
  cols={"external_id": "Option group ID (unique).", "menu_item_external_id": "Product the option group belongs to.",
   "title": "Option group title shown to customers.", "position": "Display order on the product.",
   "quantity": "Number of options the rule refers to (see unit).",
   "unit": "How 'quantity' applies. Values seen: exactly (pick exactly N), most (pick up to N), all.",
   "modifier_type": "Kind of option group. Values seen: addon, type, flavour.", "channel": "Where it is offered. Value seen: all."}),
 "modifier_items": dict(group="Menu", grain="One row per product available in an option group.",
  purpose="Which products can be picked in each option group.",
  refresh="Replaced with its option group on every load.",
  joins=["menu_modifiers on modifier_external_id = menu_modifiers.external_id", "menu_items on menu_item_external_id = menu_items.external_id"],
  cols={"modifier_external_id": "Option group.", "menu_item_external_id": "Product that can be picked in it."}),
 # ---- Promotions
 "discounts": dict(group="Promotions", grain="One row per discount / promo code.",
  purpose="Promotions set up in the platform and their rules. (Kenya has none at the time of writing.)",
  refresh="Full snapshot. Reloaded in full every day.",
  joins=["discount_applicable_items, discount_combo_items on discount_external_id = discounts.external_id",
         "branches on branch_external_id = branches.external_id"],
  cols={"external_id": "Discount ID (unique).", "code": "Promo code customers enter.", "type": "Kind of discount, e.g. fixed amount or percentage.",
   "target": "What the discount applies to.", "channel": "Where it can be used.", "category": "Discount category.",
   "value": "Discount value; an amount in minor units or a percentage, depending on type.",
   "start_date": "When the discount starts (UTC).", "end_date": "When it ends (UTC).",
   "is_first_time_user_only": "True if only first-time customers can use it.",
   "minimum_order_amount": "Minimum order value to qualify, in minor units.", "min_quantity_per_item": "Minimum quantity per item to qualify.",
   "partial_combo_threshold": "Share of a combo that must be bought for a partial combo discount.",
   "usage_limit": "Maximum number of uses.", "usage_count": "Number of times used so far.", "active": "True if the discount is active.",
   "partial_combo_allowed": "True if a partial combo still qualifies.", "customer_external_id": ACCOUNT,
   "branch_external_id": "Branch the discount belongs to, if branch-specific.",
   "created_at": "When the discount was created (UTC).", "updated_at": "When it last changed (UTC)."}),
 "discount_applicable_items": dict(group="Promotions", grain="One row per product a discount applies to.",
  purpose="Products a discount can be used on.", refresh="Replaced with its discount on every load.",
  joins=["discounts on discount_external_id = discounts.external_id", "menu_items on item_external_id = menu_items.external_id"],
  cols={"discount_external_id": "Discount.", "item_external_id": "Product it applies to."}),
 "discount_combo_items": dict(group="Promotions", grain="One row per product in a combo discount.",
  purpose="Products that together make up a combo deal.", refresh="Replaced with its discount on every load.",
  joins=["discounts on discount_external_id = discounts.external_id", "menu_items on item_external_id = menu_items.external_id"],
  cols={"discount_external_id": "Discount.", "item_external_id": "Product in the combo."}),
 # ---- Branches & staff
 "branches": dict(group="Branches & staff", grain="One row per branch (store).",
  purpose="Every Coldstone branch: name, location and settings.",
  refresh="Full snapshot. Reloaded in full every day.",
  joins=["Most tables link here through branch_external_id = branches.external_id"],
  cols={"external_id": "Branch ID (unique).", "branch_code": "Short branch code, where set.", "name": "Branch name (lower case, e.g. 'westlands').",
   "address": "Street address.", "latitude": "Latitude of the branch.", "longitude": "Longitude of the branch.",
   "is_active": "True if the branch is active.", "is_headquarters": "True for the head-office branch.",
   "is_private": "True if the branch is not publicly listed.", "longest_distance": "Furthest delivery distance configured for the branch.",
   "customer_external_id": ACCOUNT, "created_at": "When the branch was created (UTC).", "updated_at": "When it last changed (UTC)."}),
 "members": dict(group="Branches & staff", grain="One row per staff user.",
  purpose="Staff who use the Coldstone dashboard (store staff, admins, customer support).",
  refresh="Full snapshot. Reloaded in full every day.",
  joins=["branches on branch_external_id = branches.external_id", "branch_members, member_additional_branches, member_roles on member_external_id",
         "activity_initiators on initiator_external_id = members.external_id"],
  cols={"external_id": "Staff user ID (unique).", "email": "Email address." + PII, "username": "Login name.",
   "first_name": "First name." + PII, "last_name": "Last name." + PII,
   "role_name": "Main role. Values seen: Stores, Super Admin, Customer Support.", "is_deactivated": "True if the user has been deactivated.",
   "verified": "True if the user has verified their account.", "phone_number": "Phone number." + PII, "customer_external_id": ACCOUNT,
   "branch_external_id": "Home branch.", "created_at": "When the user was created (UTC).", "updated_at": "When it last changed (UTC)."}),
 "branch_members": dict(group="Branches & staff", grain="One row per staff user and home branch.",
  purpose="Links each staff user to their home branch.", refresh="Replaced with its staff user on every load.",
  joins=["branches on branch_external_id", "members on member_external_id"],
  cols={"branch_external_id": "Branch.", "member_external_id": "Staff user."}),
 "member_additional_branches": dict(group="Branches & staff", grain="One row per staff user and extra branch.",
  purpose="Extra branches a staff user can work in, besides their home branch.", refresh="Replaced with its staff user on every load.",
  joins=["members on member_external_id", "branches on branch_external_id"],
  cols={"member_external_id": "Staff user.", "branch_external_id": "Extra branch they can access."}),
 "member_roles": dict(group="Branches & staff", grain="One row per staff user and role.",
  purpose="Roles assigned to each staff user.", refresh="Replaced with its staff user on every load.",
  joins=["members on member_external_id", "roles on role_external_id = roles.external_id"],
  cols={"member_external_id": "Staff user.", "role_external_id": "Role."}),
 "roles": dict(group="Branches & staff", grain="One row per role.", purpose="Staff roles, e.g. Stores, Super Admin, Customer Support.",
  refresh="Full snapshot. Reloaded in full every day.", joins=["member_roles on role_external_id = roles.external_id"],
  cols={"external_id": "Role ID (unique).", "name": "Role name.", "description": "What the role is for.",
   "created_at": "When the role was created (UTC).", "updated_at": "When it last changed (UTC)."}),
 "permissions": dict(group="Branches & staff", grain="One row per permission.", purpose="Areas of the dashboard a role can be given access to.",
  refresh="Full snapshot. Reloaded in full every day.", joins=["sub_permissions on permission_external_id = permissions.external_id"],
  cols={"external_id": "Permission ID (unique).", "name": "Permission name.", "description": "What it allows.",
   "created_at": "When it was created (UTC).", "updated_at": "When it last changed (UTC)."}),
 "sub_permissions": dict(group="Branches & staff", grain="One row per detailed permission.", purpose="Finer-grained actions within a permission.",
  refresh="Full snapshot. Reloaded in full every day.", joins=["permissions on permission_external_id = permissions.external_id"],
  cols={"external_id": "Sub-permission ID (unique).", "name": "Name.", "description": "What it allows.",
   "permission_external_id": "Parent permission.", "is_enabled": "True if enabled.",
   "created_at": "When it was created (UTC).", "updated_at": "When it last changed (UTC)."}),
 # ---- Activity
 "activity_logs": dict(group="Activity (audit trail)", grain="One row per action taken in the platform.",
  purpose="An audit trail of actions: logins, menu changes such as marking items out of stock, and customer actions.",
  refresh="Incremental. New actions are added every day.",
  joins=["activity_initiators on activity_external_id = activity_logs.external_id", "branches on branch_external_id = branches.external_id"],
  cols={"external_id": "Action ID (unique).", "description": "What happened, e.g. 'Logged in to Cold Stone Creamery'.",
   "customer_external_id": ACCOUNT, "branch_external_id": "Branch the action relates to.",
   "initiator_type": "Who did it. Values seen: Member (staff), Customer.",
   "created_at": "When the action happened (UTC).", "updated_at": "When the record last changed (UTC)."}),
 "activity_initiators": dict(group="Activity (audit trail)", grain="One row per person behind an action.",
  purpose="The person who performed each action in activity_logs.", refresh="Replaced with its action on every load.",
  joins=["activity_logs on activity_external_id = activity_logs.external_id", "members on initiator_external_id = members.external_id (staff)"],
  cols={"activity_external_id": "The action.", "initiator_external_id": "Person who did it (staff user or customer ID).",
   "first_name": "First name." + PII, "last_name": "Last name." + PII, "username": "Login name.", "email": "Email address." + PII,
   "role_name": "Their role at the time. Values seen: Stores, Super Admin, Customer Support.",
   "initiator_type": "Member (staff) or Customer."}),
}
GROUP_ORDER = ["Sales", "Customers", "Wallet & settlement", "Menu", "Promotions", "Branches & staff", "Activity (audit trail)"]
LOADED_AT = "When this row was last written by the daily load (UTC). Set by the pipeline, not the source system."

# ---------------------------------------------------------------- read the real columns, keys and types from the DDL
ddl = open(DDL, encoding="utf-8").read()
DEFS = {}
for m in re.finditer(r'CREATE TABLE IF NOT EXISTS coldstone_web_ng_analytics\.("?\w+"?) \((.*?)\n\)\s*(.*?);', ddl, re.S):
    table, body, tail = m.group(1).strip('"'), m.group(2), " ".join(m.group(3).split())
    cols, pk = [], ""
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        if line.startswith("PRIMARY KEY"):
            pk = line[line.index("(") + 1:line.rindex(")")]
            continue
        parts = line.split()
        name, typ = parts[0], parts[1]
        if typ in ("DOUBLE",):
            typ = "DOUBLE PRECISION"
        ref = re.search(r"REFERENCES (\S+) \((\w+)\)", line)
        cols.append((name, typ, "NOT NULL" in line, ref.group(1).split(".")[1].strip('"') if ref else None))
    DEFS[table] = dict(cols=cols, pk=pk, storage=tail)
missing = set(TABLES) ^ set(DEFS)
assert not missing, missing
for t, d in DEFS.items():
    undocumented = [c for c, *_ in d["cols"] if c != "loaded_at" and c not in TABLES[t]["cols"]]
    assert not undocumented, (t, undocumented)

# ---------------------------------------------------------------- page furniture
def on_page(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 7.5)
    canvas.setFillColor(MUTED)
    canvas.drawString(18 * mm, 10 * mm, "Coldstone Web data warehouse - data dictionary")
    canvas.drawRightString(A4[0] - 18 * mm, 10 * mm, f"Page {doc.page}")
    canvas.setStrokeColor(LINE)
    canvas.line(18 * mm, 13 * mm, A4[0] - 18 * mm, 13 * mm)
    canvas.restoreState()


def grid(rows, widths, header=True, mono_cols=(), zebra=True):
    data = []
    for i, r in enumerate(rows):
        out = []
        for j, v in enumerate(r):
            if isinstance(v, str):
                style = "cellb" if (header and i == 0) else ("cellm" if j in mono_cols else "cell")
                v = Paragraph(v, S[style])
            out.append(v)
        data.append(out)
    t = Table(data, colWidths=widths, repeatRows=1 if header else 0)
    st = [("VALIGN", (0, 0), (-1, -1), "TOP"), ("GRID", (0, 0), (-1, -1), 0.4, LINE),
          ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
          ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]
    if header:
        st.append(("BACKGROUND", (0, 0), (-1, 0), HEAD_BG))
    if zebra:
        for i in range(1 if header else 0, len(rows)):
            if i % 2 == 0:
                st.append(("BACKGROUND", (0, i), (-1, i), ALT_BG))
    t.setStyle(TableStyle(st))
    return t


W = A4[0] - 36 * mm
story = []

# ---------------------------------------------------------------- cover / overview
story += [Spacer(1, 30 * mm), P("Coldstone Web data warehouse", "title"), Spacer(1, 3 * mm),
          P("Data dictionary for the Nigeria and Kenya schemas", "subtitle"), Spacer(1, 10 * mm),
          grid([["Database", "coldstone_web"],
                ["Platform", "Amazon Redshift Serverless, workgroup eatngo-analytics (us-east-1)"],
                ["Nigeria tables", "coldstone_web_ng_analytics"],
                ["Kenya tables", "coldstone_web_ke_analytics"],
                ["Source", "Coldstone Creamery online ordering platform (API), Nigeria and Kenya"],
                ["Refreshed", "Daily, with the previous day's data (D-1); history from each country's first order"],
                ["Tables", "24 per country, identical in both schemas"]],
               [38 * mm, W - 38 * mm], header=False, zebra=False),
          Spacer(1, 10 * mm),
          P("This document explains what each table and column in the Coldstone warehouse contains, how the tables "
            "connect, and the conventions to keep in mind when querying them. It is written for anyone building reports or "
            "answering questions with this data.", "body"),
          PageBreak()]

story += [P("1. The two schemas", "h1"),
          P("Nigeria and Kenya have their own copy of the same 24 tables. Column names, types and meanings are identical; "
            "only the data and the currency differ. To compare countries, query both schemas and combine the results "
            "(for example with UNION ALL), adding a country column of your own.", "body"),
          grid([["Schema", "Country", "Currency", "Use it for"],
                ["coldstone_web_ng_analytics", "Nigeria", "Naira (NGN), stored in kobo", "Reports and analysis"],
                ["coldstone_web_ke_analytics", "Kenya", "Kenyan shilling (KES), stored in cents", "Reports and analysis"],
                ["coldstone_web_ng_staging", "Nigeria", "-", "Internal to the load. Do not query: emptied and refilled every run."],
                ["coldstone_web_ke_staging", "Kenya", "-", "Internal to the load. Do not query."]],
               [50 * mm, 20 * mm, 45 * mm, W - 115 * mm], mono_cols=(0,)),
          P("2. Things to know before querying", "h1")]
rules = [
 ("Money is in minor units", "All amounts (prices, totals, balances) are whole numbers of kobo (Nigeria) or cents (Kenya). "
  "Divide by 100 to get naira or shillings: <font face='Courier'>total_price / 100.0</font>. Kenya amounts are in KES and are "
  "never converted to naira."),
 ("Times are in UTC", "Every timestamp is stored in UTC. Business days follow local time: Lagos is UTC+1 and Nairobi is UTC+3. "
  "Convert before grouping by day: <font face='Courier'>CONVERT_TIMEZONE('UTC', 'Africa/Lagos', created_at)</font> "
  "(use 'Africa/Nairobi' for Kenya). An order at 00:30 in Lagos is stored as 23:30 the previous day."),
 ("IDs", "Every record has a 24-character ID from the source platform. In each table it is called "
  "<font face='Courier'>external_id</font>; in other tables the same ID appears as <font face='Courier'>&lt;thing&gt;_external_id</font> "
  "(e.g. <font face='Courier'>orders.branch_external_id</font> = <font face='Courier'>branches.external_id</font>)."),
 ("Latest state, no duplicates", "Each order, customer and so on appears once, showing its latest known state. When an order's "
  "status changes, its row is updated rather than a new row being added. Line tables (order items, options, timeline) are "
  "replaced together with their order."),
 ("Daily refresh (D-1)", "Each morning the pipeline loads everything created on the previous day (00:00-24:00 local time), plus "
  "older records that changed. Today's activity appears tomorrow. Reference tables (branches, menu, staff, roles, wallets, "
  "discounts) are reloaded in full every day."),
 ("loaded_at", "Every table has a <font face='Courier'>loaded_at</font> column: when the pipeline last wrote that row (UTC). It is "
  "not a business date; use <font face='Courier'>created_at</font> for when something happened."),
 ("Personal data", "Columns marked <i>(personal data)</i> hold names, emails, phone numbers or bank details. Only use them where "
  "you have a legitimate need, and keep them out of widely shared reports."),
 ("Not included", "Chowdeck orders (handled by a separate pipeline) and customer messages (not available from the source)."),
]
story.append(grid([["Topic", "What it means"]] + [[f"<b>{a}</b>", b] for a, b in rules], [38 * mm, W - 38 * mm]))
story.append(PageBreak())

# ---------------------------------------------------------------- table index
story.append(P("3. Tables at a glance", "h1"))
story.append(P("Grouped by subject. 'Incremental' tables grow day by day; 'full snapshot' tables are reloaded in full every day; "
               "line tables are replaced together with the record they belong to.", "body"))
rows = [["Table", "Subject", "One row per", "What it holds"]]
for g in GROUP_ORDER:
    for t, d in TABLES.items():
        if d["group"] == g:
            rows.append([t, g, d["grain"].replace("One row per ", "").rstrip("."), d["purpose"]])
story.append(grid(rows, [50 * mm, 30 * mm, 36 * mm, W - 116 * mm], mono_cols=(0,)))

story.append(P("4. How the tables connect", "h1"))
story.append(P("The main joins. Every arrow is 'many rows on the left belong to one row on the right'.", "body"))
links = [["From", "Column", "To"],
 ["order_items, order_item_modifiers, order_timeline", "order_external_id", "orders.external_id"],
 ["order_item_modifiers", "order_external_id, line_no", "order_items (same two columns)"],
 ["orders, customers, wallets, wallet_transactions, menu_items, members, activity_logs", "branch_external_id", "branches.external_id"],
 ["orders", "customer_external_id", "customers.external_id"],
 ["order_items", "product_external_id", "menu_items.external_id"],
 ["menu_items", "category_external_id", "menu_categories.external_id"],
 ["menu_modifiers", "menu_item_external_id", "menu_items.external_id"],
 ["modifier_items", "modifier_external_id / menu_item_external_id", "menu_modifiers / menu_items"],
 ["discount_applicable_items, discount_combo_items", "discount_external_id / item_external_id", "discounts / menu_items"],
 ["branch_members, member_additional_branches, member_roles", "member_external_id", "members.external_id"],
 ["member_roles", "role_external_id", "roles.external_id"],
 ["sub_permissions", "permission_external_id", "permissions.external_id"],
 ["activity_initiators", "activity_external_id", "activity_logs.external_id"]]
story.append(grid(links, [70 * mm, 55 * mm, W - 125 * mm], mono_cols=(0, 1, 2)))
story.append(PageBreak())

# ---------------------------------------------------------------- per-table pages
story.append(P("5. Table reference", "h1"))
story.append(P("Each table below exists in both <font face='Courier'>coldstone_web_ng_analytics</font> and "
               "<font face='Courier'>coldstone_web_ke_analytics</font>. 'Key' marks the columns that uniquely identify a row.", "body"))
n = 0
for g in GROUP_ORDER:
    for t, d in TABLES.items():
        if d["group"] != g:
            continue
        n += 1
        ddl_t = DEFS[t]
        pk = [c.strip() for c in ddl_t["pk"].split(",")]
        head = [P(f"5.{n}  {t}", "h2"),
                P(f"<b>{g}.</b> {d['purpose']}", "body"),
                grid([["One row per", d["grain"].replace("One row per ", "").rstrip(".")],
                      ["Unique key", ", ".join(pk)],
                      ["Refresh", d["refresh"]],
                      ["Joins to", "<br/>".join(d["joins"])],
                      ["Storage", ddl_t["storage"].replace("DISTSTYLE ALL", "copied to every node (DISTSTYLE ALL)")
                                  .replace("COMPOUND SORTKEY", "sorted by").replace("SORTKEY", "sorted by").replace("DISTSTYLE AUTO", "distribution chosen by Redshift").replace("DISTKEY", "distributed by")]],
                     [28 * mm, W - 28 * mm], header=False, zebra=False)]
        rows = [["Column", "Type", "Description"]]
        for name, typ, notnull, ref in ddl_t["cols"]:
            desc = LOADED_AT if name == "loaded_at" else d["cols"][name]
            flags = []
            if name in pk:
                flags.append("<b>Key</b>")
            if ref:
                flags.append(f"links to {ref}")
            label = name + (f"<br/><font size='6.5' color='#5f6b7a'>{' · '.join(flags)}</font>" if flags else "")
            rows.append([label, typ.replace("TIMESTAMP", "TIMESTAMP (UTC)"), desc])
        story += [KeepTogether(head), Spacer(1, 2 * mm),
                  grid(rows, [44 * mm, 33 * mm, W - 77 * mm], mono_cols=(0, 1)), Spacer(1, 4 * mm)]
        story.append(CondPageBreak(60 * mm))

# ---------------------------------------------------------------- examples
story.append(PageBreak())
story.append(P("6. Example queries", "h1"))
examples = [
 ("Daily sales in Nigeria, by Lagos business day",
  "SELECT CAST(CONVERT_TIMEZONE('UTC', 'Africa/Lagos', created_at) AS DATE) AS business_date,\n"
  "       COUNT(*)                     AS orders,\n"
  "       SUM(total_price) / 100.0     AS sales_ngn\n"
  "FROM coldstone_web_ng_analytics.orders\n"
  "WHERE payment_status = 'paid'\n"
  "GROUP BY 1\nORDER BY 1 DESC;"),
 ("Top 10 Kenya branches by sales in a month (amounts in KES)",
  "SELECT b.name AS branch, COUNT(*) AS orders, SUM(o.total_price) / 100.0 AS sales_kes\n"
  "FROM coldstone_web_ke_analytics.orders o\n"
  "JOIN coldstone_web_ke_analytics.branches b ON b.external_id = o.branch_external_id\n"
  "WHERE o.payment_status = 'paid'\n"
  "  AND o.created_at >= '2026-08-31 21:00'   -- 1 Sep 00:00 Nairobi, in UTC\n"
  "  AND o.created_at <  '2026-09-30 21:00'   -- 1 Oct 00:00 Nairobi, in UTC\n"
  "GROUP BY 1\nORDER BY 3 DESC\nLIMIT 10;"),
 ("Best-selling products in Nigeria",
  "SELECT i.product_name, SUM(i.quantity) AS units\n"
  "FROM coldstone_web_ng_analytics.order_items i\n"
  "JOIN coldstone_web_ng_analytics.orders o ON o.external_id = i.order_external_id\n"
  "WHERE o.payment_status = 'paid'\n"
  "GROUP BY 1\nORDER BY 2 DESC\nLIMIT 20;"),
 ("Average time from order to delivery (Kenya, minutes)",
  "SELECT AVG(DATEDIFF(minute, o.created_at, t.updated_at)) AS minutes_to_deliver\n"
  "FROM coldstone_web_ke_analytics.orders o\n"
  "JOIN coldstone_web_ke_analytics.order_timeline t ON t.order_external_id = o.external_id\n"
  "WHERE t.title = 'delivered' AND t.status;"),
 ("Both countries side by side (each in its own currency)",
  "SELECT 'NG' AS country, COUNT(*) AS orders, SUM(total_price) / 100.0 AS sales_local\n"
  "FROM coldstone_web_ng_analytics.orders WHERE payment_status = 'paid'\n"
  "UNION ALL\n"
  "SELECT 'KE', COUNT(*), SUM(total_price) / 100.0\n"
  "FROM coldstone_web_ke_analytics.orders WHERE payment_status = 'paid';"),
]
for title, sql in examples:
    code = Paragraph(sql.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\n", "<br/>")
                        .replace("  ", "&nbsp;&nbsp;"), S["code"])
    box = Table([[code]], colWidths=[W])
    box.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), ALT_BG), ("BOX", (0, 0), (-1, -1), 0.4, LINE),
                             ("LEFTPADDING", (0, 0), (-1, -1), 6), ("TOPPADDING", (0, 0), (-1, -1), 5),
                             ("BOTTOMPADDING", (0, 0), (-1, -1), 5)]))
    story.append(KeepTogether([P(title, "h2"), box]))
story.append(Spacer(1, 4 * mm))
story.append(P("Where a description says 'values seen', the list comes from the data loaded so far and may grow as the "
               "platform adds new statuses or options.", "meta"))

doc = BaseDocTemplate(OUT, pagesize=A4, leftMargin=18 * mm, rightMargin=18 * mm, topMargin=16 * mm, bottomMargin=18 * mm,
                      title="Coldstone Web data warehouse - data dictionary", author="Data team")
doc.addPageTemplates([PageTemplate(id="p", frames=[Frame(18 * mm, 18 * mm, W, A4[1] - 34 * mm, id="f")], onPage=on_page)])
doc.build(story)
print("written", OUT)
