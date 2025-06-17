def build_card(label:str, total:float):
    formatted_total = f"{total:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"""
            <div style="
                background-color: #f0f2f6;
                border: 2px solid #007bff;
                border-radius: 10px;
                padding: 15px;
                text-align: center;
                box-shadow: 2px 2px 8px rgba(0,0,0,0.2);
                margin-bottom: 20px;
            ">
                <h3 style="color: #333; margin-bottom: 5px;">{label}</h3>
                <p style="font-size: 2.0em; font-weight: bold; color: #007bff; margin: 0;">
                    {formatted_total}
                </p>
            </div>
            """