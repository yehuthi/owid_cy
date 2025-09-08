import owid_cy
import sys

def main():
    df = owid_cy.agg()
    df.to_csv(sys.stdout, index=True)
if __name__ == '__main__': main()
